#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest


CHECK = pathlib.Path(__file__).with_name("check_buf_breaking.sh")
SCHEMA = '''syntax = "proto3";
package stargate;
service StargateControlPlane {
  rpc SubmitClusterCalibration(SubmitClusterCalibrationRequest) returns (SubmitClusterCalibrationResponse);
}
enum CalibrationState { UNKNOWN = 0; READY = 1; }
message InferenceServerRegistration { string id = 1; bool coordinated_calibration = 6; }
message InferenceServerAck { repeated ModelCalibrationDirective model_calibration_directives = 2; }
message ModelCalibrationDirective {}
message SubmitClusterCalibrationRequest {}
message SubmitClusterCalibrationResponse {}
'''


class BreakingCheckTest(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(shutil.which("buf"), "install Buf before running these tests")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = pathlib.Path(self.temp.name)
        self.workspace = self.repo / "src/libraries/rust/stargate"
        self.proto = self.workspace / "crates/proto/proto/stargate.proto"
        self.proto.parent.mkdir(parents=True)
        self.proto.write_text(SCHEMA)
        self.script = self.workspace / "scripts/check_buf_breaking.sh"
        self.script.parent.mkdir()
        shutil.copyfile(CHECK, self.script)
        self.git("init", "-q")
        self.git("add", ".")
        self.git("-c", "user.name=Contract Test", "-c", "user.email=test@example.com",
                 "-c", "commit.gpgsign=false", "commit", "-q", "-s", "-m", "test: baseline")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True,
                              capture_output=True, text=True)

    def check(self, base="HEAD", cwd=None, schemas=None):
        args = ["bash", str(self.script), base]
        if schemas is not None:
            args.append(schemas)
        return subprocess.run(args,
                              cwd=cwd or self.repo, capture_output=True, text=True,
                              env={**os.environ, "XDG_CACHE_HOME": str(self.repo / "cache")})

    def test_additive_change_from_root_and_subtree(self):
        self.proto.write_text(SCHEMA.replace("string id = 1;", "string id = 1; optional double capacity = 2;"))
        for cwd in (self.repo, self.workspace):
            with self.subTest(cwd=cwd):
                result = self.check(cwd=cwd)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_breaking_changes_fail(self):
        changes = {
            "field deletion": SCHEMA.replace("bool coordinated_calibration = 6;", ""),
            "field reuse": SCHEMA.replace("string id = 1;", "uint64 id = 1;"),
            "field renumber": SCHEMA.replace("string id = 1;", "string id = 2;"),
            "RPC deletion": SCHEMA.replace(
                "rpc SubmitClusterCalibration(SubmitClusterCalibrationRequest) returns (SubmitClusterCalibrationResponse);", ""),
            "stream change": SCHEMA.replace("(SubmitClusterCalibrationRequest)", "(stream SubmitClusterCalibrationRequest)"),
            "enum deletion": SCHEMA.replace("enum CalibrationState { UNKNOWN = 0; READY = 1; }", ""),
            "file deletion": "",
        }
        for name, schema in changes.items():
            with self.subTest(change=name):
                self.proto.write_text(schema)
                result = self.check()
                self.assertNotEqual(result.returncode, 0, name)
                self.assertIn("Previously present" if "deletion" in name or "renumber" in name else "changed",
                              result.stdout + result.stderr)

    def test_calibration_removal_is_not_exempt(self):
        removed = SCHEMA
        for line in SCHEMA.splitlines():
            if any(name in line for name in (
                "rpc SubmitClusterCalibration", "enum CalibrationState",
                "message ModelCalibrationDirective", "message SubmitClusterCalibration",
            )):
                removed = removed.replace(line, "")
        removed = removed.replace("bool coordinated_calibration = 6;", "")
        removed = removed.replace("repeated ModelCalibrationDirective model_calibration_directives = 2;", "")
        self.proto.write_text(removed)
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SubmitClusterCalibration", result.stdout + result.stderr)
        self.assertIn("coordinated_calibration", result.stdout + result.stderr)

    def test_candidate_config_cannot_disable_rules(self):
        (self.workspace / "buf.yaml").write_text(
            "version: v2\nbreaking:\n  use: [WIRE]\n  ignore: [crates/proto/proto]\n")
        self.proto.write_text(SCHEMA.replace("bool coordinated_calibration = 6;", ""))
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("coordinated_calibration", result.stdout + result.stderr)

    def test_missing_baseline_fails(self):
        self.assertNotEqual(self.check("missing-ref").returncode, 0)

    def test_missing_base_schema_fails(self):
        self.git("rm", "-r", "src/libraries/rust/stargate/crates")
        self.git("-c", "user.name=Contract Test", "-c", "user.email=test@example.com",
                 "-c", "commit.gpgsign=false", "commit", "-q", "-s", "-m", "test: no schema")
        self.proto.parent.mkdir(parents=True)
        self.proto.write_text(SCHEMA)
        self.assertNotEqual(self.check().returncode, 0)

    def test_gateway_server_and_client_copies_are_checked_independently(self):
        paths = ["src/control-plane-services/cloud-functions/nvcf-core/src/main/proto",
                 "src/invocation-plane-services/llm-api-gateway/nvcf/pb"]
        for path in paths:
            proto = self.repo / path / "llm_gateway.proto"
            proto.parent.mkdir(parents=True)
            proto.write_text(SCHEMA)
        self.git("add", ".")
        self.git("-c", "user.name=Contract Test", "-c", "user.email=test@example.com",
                 "-c", "commit.gpgsign=false", "commit", "-q", "-s", "-m", "test: gateway copies")
        for path in paths:
            with self.subTest(path=path):
                proto = self.repo / path / "llm_gateway.proto"
                proto.write_text(SCHEMA.replace("string id = 1;", "uint64 id = 1;"))
                self.assertNotEqual(self.check(schemas=path).returncode, 0)
                self.assertEqual(self.check().returncode, 0)
                proto.write_text(SCHEMA)
                self.assertEqual(self.check(schemas=path).returncode, 0)

    def test_gateway_file_scope_excludes_core_api_changes(self):
        path = "src/control-plane-services/cloud-functions/nvcf-core/src/main/proto"
        directory = self.repo / path
        directory.mkdir(parents=True)
        gateway = directory / "llm_gateway.proto"
        gateway.write_text(SCHEMA)
        core = directory / "nvcf.proto"
        core.write_text('syntax = "proto3"; package nvcf; message Core { string id = 1; }')
        self.git("add", ".")
        self.git("-c", "user.name=Contract Test", "-c", "user.email=test@example.com",
                 "-c", "commit.gpgsign=false", "commit", "-q", "-s", "-m", "test: core and gateway")
        core.write_text('syntax = "proto3"; package nvcf; message Core {}')
        self.assertNotEqual(self.check(schemas=path).returncode, 0)
        self.assertEqual(self.check(schemas=path + "/llm_gateway.proto").returncode, 0)
        gateway.write_text(SCHEMA.replace("string id = 1;", "uint64 id = 1;"))
        self.assertNotEqual(self.check(schemas=path + "/llm_gateway.proto").returncode, 0)


if __name__ == "__main__":
    unittest.main()
