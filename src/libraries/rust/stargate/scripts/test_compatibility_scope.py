#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pathlib
import os
import subprocess
import textwrap
import unittest


SCRIPT = pathlib.Path(__file__).with_name("compatibility_scope.sh")


class CompatibilityScopeTest(unittest.TestCase):
    def scope(self, *args, paths=""):
        return subprocess.run(["bash", str(SCRIPT), *args], input=paths, text=True,
                              capture_output=True, check=True).stdout.strip()

    def test_unrelated_changes_do_not_require_release_services(self):
        for path in ["docs/guide.md", "AGENTS.md",
                     "src/libraries/rust/stargate/docs/load-balancer-configuration.md",
                     "src/libraries/rust/stargate/AGENTS.md",
                     "src/control-plane-services/cloud-functions/nvcf-core/src/main/java/Service.java",
                     "src/control-plane-services/cloud-functions/nvcf-core/src/main/proto/nvcf.proto"]:
            with self.subTest(path=path):
                self.assertEqual(self.scope("paths", "false", paths=path), "false")

    def test_contract_and_wiring_changes_run_checks(self):
        for path in ["src/libraries/rust/stargate/crates/proto/proto/stargate.proto",
                     "src/libraries/rust/stargate/crates/stargate/src/lib.rs",
                     "src/libraries/rust/stargate/scripts/compatibility_scope.sh",
                     "src/control-plane-services/cloud-functions/nvcf-core/src/main/proto/llm_gateway.proto",
                     "src/invocation-plane-services/llm-api-gateway/nvcf/pb/llm_gateway.proto",
                     ".github/workflows/bazel.yml"]:
            with self.subTest(path=path):
                self.assertEqual(self.scope("paths", "false", paths="docs/guide.md\n" + path), "true")

    def test_unknown_diff_runs_checks(self):
        self.assertEqual(self.scope("paths", "true"), "true")

    def test_maintenance_branches_do_not_compare_to_newer_release_schemas(self):
        for target in ["release-src/libraries/rust/stargate/v0.18", "refs/heads/release/v0.18"]:
            self.assertEqual(self.scope("releases", target, "main"), "false")
        for target in ["main", "refs/heads/main"]:
            self.assertEqual(self.scope("releases", target, "main"), "true")
        self.assertEqual(self.scope("releases", "refs/heads/trunk", "trunk"), "true")
        for target, default in [("", "main"), ("main", "")]:
            with self.assertRaises(subprocess.CalledProcessError):
                self.scope("releases", target, default)

    def test_selected_contract_job_remains_required_without_blocking_build_jobs(self):
        workflow = (SCRIPT.parents[5] / ".github/workflows/bazel.yml").read_text()
        check = workflow.split("- name: Check Bazel workflow result", 1)[1].split("run: |\n", 1)[1]
        lines = []
        for line in check.splitlines():
            if line.strip() and not line.startswith("          "):
                break
            lines.append(line)
        script = textwrap.dedent("\n".join(lines))
        env = {**os.environ, "DETECT_RESULT": "success", "BAZEL_RESULT": "success",
               "DOCKER_RESULT": "success", "BAZEL_ANY": "true"}
        for selected, result, any_bazel, success in [
                ("true", "success", "true", True), ("true", "skipped", "true", False),
                ("true", "failure", "true", False), ("true", "cancelled", "true", False),
                ("true", "failure", "false", False), ("false", "skipped", "false", True)]:
            with self.subTest(selected=selected, result=result):
                run = subprocess.run(["bash", "-c", script], capture_output=True,
                                     env={**env, "BAZEL_ANY": any_bazel, "COMPATIBILITY_SELECTED": selected,
                                          "COMPATIBILITY_RESULT": result})
                self.assertEqual(run.returncode == 0, success)


if __name__ == "__main__":
    unittest.main()
