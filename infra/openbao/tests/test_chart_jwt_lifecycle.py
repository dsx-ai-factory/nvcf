# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[3]
SCRIPTS = REPO / "deploy/helm/openbao/helm/scripts"
SHA = "1" * 64
MANIFEST = (REPO / "infra/openbao/jwt-plugin-versions.env").read_text()
VERSIONS = dict(line.split("=", 1) for line in MANIFEST.splitlines()
                if line and not line.startswith("#"))
TARGET = VERSIONS["JWT_PLUGIN_TARGET_VERSION"]
PLUGIN = b"member target plugin\n"
TARGET_SHA = hashlib.sha256(PLUGIN).hexdigest()

FAKE_KUBECTL = r'''#!/usr/bin/env python3
import json, os, subprocess, sys
from pathlib import Path
p = Path(os.environ["FAKE_KUBE_STATE"])
s = json.loads(p.read_text())
a = sys.argv[1:]
s["calls"].append(a)
p.write_text(json.dumps(s))
if "statefulset" in a:
    print(json.dumps(s["statefulset"]))
elif "pods" in a:
    labels = dict(x.split("=", 1) for x in a[a.index("-l") + 1].split(","))
    print(json.dumps({"items": [pod for pod in s["pods"]
        if all(pod["metadata"]["labels"].get(k) == v for k, v in labels.items())]}))
elif "exec" in a:
    remote = a[a.index("--") + 1:]
    if remote[:2] == ["env", "BAO_TOKEN=test-token"]:
        remote = remote[2:]
    if remote == ["sh", "-ec", "test ! -r /openbao/jwt-plugin-versions.env"]:
        sys.exit(1 if s.get("versioned_image") else 0)
    elif remote == ["test", "-x", "/openbao/plugins/vault-plugin-secrets-jwt"]:
        sys.exit(0 if s.get("legacy_binary", True) else 1)
    elif remote == ["sha256sum", "/openbao/plugins/vault-plugin-secrets-jwt"]:
        print("1" * 64 + "  plugin")
    elif remote == ["bao", "plugin", "info", "-format=json", "secret", "vault-plugin-secrets-jwt"]:
        print(json.dumps(s["catalog"]))
    elif remote == ["bao", "plugin", "register", "-sha256=" + "1" * 64,
                    "-command=vault-plugin-secrets-jwt", "secret", "vault-plugin-secrets-jwt"]:
        s["writes"] += 1
        p.write_text(json.dumps(s))
    elif remote == ["jwt-plugin-lifecycle", "wait-ready"]:
        sys.exit(0)
    elif remote[:2] == ["sh", "-ec"] and len(remote) == 6:
        pod = a[a.index("exec") + 1]
        config = s.get("member_checks", {}).get(pod, {})
        if "exec_error" in config:
            print(config["exec_error"], file=sys.stderr)
            sys.exit(1)
        root = p.parent / "members" / pod
        # Map container paths to fixtures; execute the production shell payload.
        script = remote[2].replace("/openbao/", str(root) + "/")
        env = {**os.environ, "FAKE_MEMBER_NAME": pod,
               "PATH": str(p.parent / "member-bin") + os.pathsep + os.environ["PATH"]}
        sys.exit(subprocess.run([*remote[:2], script, *remote[3:]], env=env).returncode)
    else:
        print("Unsupported exec command: " + repr(remote), file=sys.stderr)
        sys.exit(2)
else:
    sys.exit(2)
'''

MEMBER_COMMAND = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
state = json.loads(Path(os.environ["FAKE_KUBE_STATE"]).read_text())
config = state.get("member_checks", {}).get(os.environ["FAKE_MEMBER_NAME"], {})
command = Path(sys.argv[0]).name
if command == "bao" and sys.argv[1:] == ["status", "-format=json"]:
    assert os.environ["BAO_ADDR"] == "http://127.0.0.1:8200"
    print(config.get("health_response", json.dumps(config.get("health", {
        "initialized": True, "sealed": False}))))
    if config.get("status_error"):
        print(config["status_error"], file=sys.stderr)
    sys.exit(config.get("status_exit", 0))
if command == "jwt-plugin-lifecycle" and sys.argv[1:] == ["image-check"]:
    if config.get("image_error"):
        print(config["image_error"], file=sys.stderr)
    sys.exit(config.get("image_exit", 0))
print("Unsupported member command", file=sys.stderr)
sys.exit(2)
'''


class ChartLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        fake = self.root / "kubectl"
        fake.write_text(FAKE_KUBECTL)
        fake.chmod(0o755)
        self.state = {"calls": [], "writes": 0, "catalog": {
            "version": "", "command": "vault-plugin-secrets-jwt", "sha256": SHA}}
        (self.root / "manifest.env").write_text(MANIFEST)
        (self.root / "target.sha").write_text(SHA)
        member_bin = self.root / "member-bin"
        member_bin.mkdir()
        for name in ("bao", "jwt-plugin-lifecycle"):
            command = member_bin / name
            command.write_text(MEMBER_COMMAND)
            command.chmod(0o755)
        self.env = {**os.environ, "PATH": str(self.root) + os.pathsep + os.environ["PATH"],
                    "FAKE_KUBE_STATE": str(self.root / "state.json"),
                    "JWT_SERVER_NAMESPACE": "tenant", "JWT_SERVER_STATEFULSET": "renamed-bao",
                    "JWT_SHARED_VERSIONS_DIR": str(self.root), "JWT_PLUGIN_LIFECYCLE_MODE": "legacy"}

    def run_script(self, args):
        path = self.root / "state.json"
        path.write_text(json.dumps(self.state))
        result = subprocess.run(args, env=self.env, text=True, capture_output=True)
        self.state = json.loads(path.read_text())
        return result

    def bootstrap(self):
        return self.run_script(["bash", "-c", '''
            source "$1"
            log_error() { echo "$*" >&2; }
            log_success() { :; }
            get_root_token() { echo test-token; }
            register_and_enable_jwt_plugin tenant renamed-bao
        ''', "sh", str(SCRIPTS / "utils/jwt-plugin.sh")])

    def test_legacy_bootstrap_refuses_bridge_and_target_only_images_without_writes(self):
        for legacy_binary in (True, False):
            with self.subTest(legacy_binary=legacy_binary):
                self.state.update(versioned_image=True, legacy_binary=legacy_binary)
                result = self.bootstrap()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("require bridge or versioned", result.stderr)
                self.assertEqual(self.state["writes"], 0)

    def test_versioned_catalog_entry_does_not_satisfy_legacy_bootstrap(self):
        self.state["catalog"]["version"] = "v99.0.0"
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state["writes"], 1)

    def test_matching_unversioned_catalog_is_idempotent(self):
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state["writes"], 0)

    def test_versioned_bootstrap_only_waits_for_declarative_registration(self):
        self.env["JWT_PLUGIN_LIFECYCLE_MODE"] = "versioned"
        self.state["versioned_image"] = True
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state["writes"], 0)
        self.assertTrue(any("wait-ready" in c for c in self.state["calls"]))

    def seed_members(self):
        labels = {"app.kubernetes.io/name": "custom-name", "app.kubernetes.io/instance": "this-release",
                  "component": "server"}
        self.state["statefulset"] = {"spec": {"replicas": 2, "selector": {"matchLabels": labels}}}
        self.state["pods"] = [
            {"metadata": {"name": name, "labels": dict(labels)},
             "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
            for name in ("renamed-bao-0", "renamed-bao-1")]
        (self.root / "target.sha").write_text(TARGET_SHA)
        for pod in self.state["pods"]:
            root = self.member_root(pod["metadata"]["name"])
            (root / "plugins").mkdir(parents=True)
            (root / "jwt-plugin-versions.env").write_text(MANIFEST)
            (root / "plugins" / ("vault-plugin-secrets-jwt-" + TARGET)).write_bytes(PLUGIN)

    def member_root(self, pod="renamed-bao-1"):
        return self.root / "members" / pod

    def check_members(self):
        return self.run_script(["sh", str(SCRIPTS / "check-jwt-members.sh")])

    def assert_member_failure(self, result, message):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)
        self.assertNotIn("Rotate member", result.stderr)
        self.assertNotIn("Required upgrade stop:", result.stderr)
        self.assertEqual(self.state["writes"], 0)

    def test_member_inventory_uses_live_selector_and_ignores_other_and_retired_pods(self):
        self.seed_members()
        for name, phase, terminating, other_release in (
                ("other-0", "Running", False, True), ("evicted", "Failed", False, False),
                ("completed", "Succeeded", False, False), ("terminating", "Running", True, False)):
            pod = json.loads(json.dumps(self.state["pods"][0]))
            pod["metadata"]["name"] = name
            pod["status"]["phase"] = phase
            if terminating:
                pod["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
            if other_release:
                pod["metadata"]["labels"]["app.kubernetes.io/instance"] = "other-release"
            self.state["pods"].append(pod)
        result = self.run_script(["sh", str(SCRIPTS / "check-jwt-members.sh")])
        self.assertEqual(result.returncode, 0, result.stderr)
        inspected = [c[c.index("exec") + 1] for c in self.state["calls"] if "exec" in c]
        self.assertEqual(inspected, ["renamed-bao-0", "renamed-bao-1"])

    def test_missing_member_stops_upgrade(self):
        self.seed_members()
        self.state["pods"].pop()
        self.assert_member_failure(self.check_members(), "found 1")

    def test_exec_failures_preserve_error_and_member_context(self):
        self.seed_members()
        for error in ("Forbidden: pods/exec is denied", "container openbao not found",
                      "Unable to connect to the server: i/o timeout"):
            with self.subTest(error=error):
                self.state["member_checks"] = {"renamed-bao-1": {"exec_error": error}}
                result = self.check_members()
                self.assert_member_failure(result, error)
                self.assertIn("tenant/renamed-bao-1 (StatefulSet renamed-bao)", result.stderr)

    def test_missing_unreadable_and_incomplete_member_manifests(self):
        self.seed_members()
        manifest = self.member_root() / "jwt-plugin-versions.env"
        manifest.unlink()
        self.assert_member_failure(self.check_members(), "cannot read JWT version manifest")
        manifest.write_text(MANIFEST)
        if os.geteuid() != 0:
            manifest.chmod(0o000)
            try:
                self.assert_member_failure(self.check_members(), "cannot read JWT version manifest")
            finally:
                manifest.chmod(0o644)
        manifest.write_text("false\n")
        self.assert_member_failure(self.check_members(), "cannot load JWT version manifest")
        manifest.write_text('JWT_PLUGIN_TARGET_VERSION="unterminated\n')
        self.assert_member_failure(self.check_members(), "cannot load JWT version manifest")
        manifest.write_text("JWT_PLUGIN_OLD_VERSION=v0.0.0\n")
        self.assert_member_failure(self.check_members(), "target version missing")

    def test_member_target_version_mismatch(self):
        self.seed_members()
        (self.member_root() / "jwt-plugin-versions.env").write_text(
            "JWT_PLUGIN_TARGET_VERSION=v0.0.0\n")
        self.assert_member_failure(self.check_members(), "target version mismatch")

    def test_sha_read_failure_and_mismatch(self):
        self.seed_members()
        plugin = self.member_root() / "plugins" / ("vault-plugin-secrets-jwt-" + TARGET)
        plugin.unlink()
        result = self.check_members()
        self.assert_member_failure(result, "cannot read target plugin SHA")
        self.assertIn(str(plugin), result.stderr)
        plugin.write_bytes(b"wrong plugin")
        self.assert_member_failure(self.check_members(), "target plugin SHA mismatch")

    def test_image_configuration_failure_preserves_error(self):
        self.seed_members()
        self.state["member_checks"] = {"renamed-bao-1": {
            "image_exit": 1, "image_error": "fixture: plugin registration configuration invalid"}}
        result = self.check_members()
        self.assert_member_failure(result, "image configuration validation failed")
        self.assertIn("fixture: plugin registration configuration invalid", result.stderr)

    def test_uninitialized_and_sealed_members(self):
        self.seed_members()
        for initialized, sealed, message in (
                (False, True, "OpenBao is not initialized"),
                (True, True, "OpenBao is sealed")):
            with self.subTest(initialized=initialized, sealed=sealed):
                self.state["member_checks"] = {"renamed-bao-1": {
                    "health": {"initialized": initialized, "sealed": sealed}, "status_exit": 2}}
                self.assert_member_failure(self.check_members(), message)

    def test_invalid_health_responses(self):
        self.seed_members()
        for response in ("not JSON", "", "{}", "null", "[]",
                         '{"initialized": "true", "sealed": false}'):
            with self.subTest(response=response):
                self.state["member_checks"] = {"renamed-bao-1": {"health_response": response}}
                self.assert_member_failure(self.check_members(), "invalid OpenBao health response")

    def test_failed_status_with_empty_or_healthy_response(self):
        self.seed_members()
        self.state["member_checks"] = {"renamed-bao-1": {
            "status_exit": 1, "health_response": "", "status_error": "connection refused"}}
        result = self.check_members()
        self.assert_member_failure(result, "cannot read OpenBao health")
        self.assertIn("connection refused", result.stderr)
        self.state["member_checks"] = {"renamed-bao-1": {"status_exit": 1}}
        self.assert_member_failure(self.check_members(), "OpenBao status command failed (exit 1)")

    def test_compatible_bridge_and_target_only_members_pass(self):
        self.seed_members()
        for target_only in (False, True):
            with self.subTest(target_only=target_only):
                for pod in self.state["pods"]:
                    manifest = self.member_root(pod["metadata"]["name"]) / "jwt-plugin-versions.env"
                    manifest.write_text(MANIFEST + (
                        "\nJWT_PLUGIN_BRIDGE_IMAGE=qualified-bridge\n" if target_only else ""))
                result = self.check_members()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.state["writes"], 0)

    def test_unknown_exec_command_fails_closed(self):
        result = self.run_script(["kubectl", "exec", "renamed-bao-0", "--", "unexpected"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unsupported exec command", result.stderr)

    def test_unready_member_blocks_versioned_mode(self):
        self.seed_members()
        self.state["pods"][1]["status"]["conditions"][0]["status"] = "False"
        result = self.run_script(["sh", str(SCRIPTS / "check-jwt-members.sh")])
        self.assert_member_failure(result, "must be Ready")
        self.assertIn("not Ready: renamed-bao-1", result.stderr)
        self.assertFalse(any("exec" in call for call in self.state["calls"]))

    def test_target_only_rejects_a_rollback_migration_override_before_member_exec(self):
        self.seed_members()
        manifest = self.root / "manifest.env"
        manifest.write_text(manifest.read_text() + "\nJWT_PLUGIN_BRIDGE_IMAGE=qualified-bridge\n")
        versions = dict(line.split("=", 1) for line in manifest.read_text().splitlines()
                        if line and not line.startswith("#"))
        self.env["BAO_JWT_PLUGIN_VERSION"] = versions["JWT_PLUGIN_OLD_VERSION"]
        result = self.run_script(["sh", str(SCRIPTS / "check-jwt-members.sh")])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("remove the old-version override", result.stderr)
        self.assertFalse(any("exec" in call for call in self.state["calls"]))
        self.env["BAO_JWT_PLUGIN_VERSION"] = versions["JWT_PLUGIN_TARGET_VERSION"]
        result = self.run_script(["sh", str(SCRIPTS / "check-jwt-members.sh")])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_baseline_matches_chart_and_manifest(self):
        dockerfile = (REPO / "infra/openbao/Dockerfile").read_text()
        image = re.search(r"^ARG JWT_LEGACY_IMAGE=(.+)$", dockerfile, re.M)[1]
        baseline = re.search(r":([^:@]+)@sha256:[0-9a-f]{64}$", image)[1]
        manifest = (self.root / "manifest.env").read_text()
        self.assertIn("JWT_PLUGIN_OLD_VERSION=v" + baseline.split("-nv-")[1], manifest)
        values = (REPO / "deploy/helm/openbao/helm/values.yaml").read_text()
        mode = re.search(r"jwtPluginLifecycle:\s*\n\s+mode:\s+(\S+)", values)[1]
        if mode == "legacy":
            chart = (REPO / "deploy/helm/openbao/helm/Chart.yaml").read_text()
            self.assertEqual(baseline, re.search(r"^appVersion: (\S+)", chart, re.M)[1])


if __name__ == "__main__":
    unittest.main()
