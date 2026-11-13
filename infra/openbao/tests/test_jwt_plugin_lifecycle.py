# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1]
PLUGIN = "vault-plugin-secrets-jwt"
VERSIONS = dict(
    line.split("=", 1)
    for line in (SOURCE / "jwt-plugin-versions.env").read_text().splitlines()
    if line and not line.startswith("#")
)
OLD = VERSIONS["JWT_PLUGIN_OLD_VERSION"]
TARGET = VERSIONS["JWT_PLUGIN_TARGET_VERSION"]

FAKE_BAO = r"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
path = Path(os.environ["FAKE_BAO_STATE"])
state = json.loads(path.read_text())
args = sys.argv[1:]
if state.get("fail_operation") == " ".join(args[:2]):
    print(state.get("read_error", "Injected read failure"), file=sys.stderr)
    sys.exit(2)
if state.get("fail_reads"):
    sys.exit(2)
if args[:2] == ["plugin", "list"] and state.get("catalog_list_failures", 0):
    state["catalog_list_failures"] -= 1
    path.write_text(json.dumps(state))
    sys.exit(2)
if args[:2] == ["plugin", "info"] and state.get("not_ready_reads", 0):
    state["not_ready_reads"] -= 1
    path.write_text(json.dumps(state))
    sys.exit(2)
if args[:1] == ["status"]:
    print(json.dumps(state.get("health", {"initialized": True, "sealed": False, "ha_enabled": False})))
elif args[:2] == ["plugin", "list"]:
    if "raw_catalog_response" in state:
        print(state["raw_catalog_response"])
    else:
        print(json.dumps(state.get("catalog_response", {"details": [
            {"name": "vault-plugin-secrets-jwt", **entry} for entry in state["catalog"].values()]})))
elif args[:2] == ["secrets", "list"]:
    print(state["raw_mount_response"] if "raw_mount_response" in state else json.dumps(state["mounts"]))
elif args[:2] == ["plugin", "info"]:
    version = next((x.split("=", 1)[1] for x in args if x.startswith("-version=")), "")
    if version not in state["catalog"]:
        sys.exit(2)
    print(json.dumps(state["catalog"][version]))
elif args[:2] == ["secrets", "tune"]:
    state["writes"].append(args)
    version = next(x.split("=", 1)[1] for x in args if x.startswith("-plugin-version="))
    state["mounts"][args[-1]]["plugin_version"] = version
elif args[:2] == ["plugin", "deregister"]:
    state["writes"].append(args)
    version = next((x.split("=", 1)[1] for x in args if x.startswith("-version=")), "")
    if state.get("fail_delete_version") == version:
        state.pop("fail_delete_version")
        path.write_text(json.dumps(state))
        sys.exit(2)
    state["catalog"].pop(version, None)
else:
    sys.exit("unexpected bao arguments: " + repr(args))
path.write_text(json.dumps(state))
"""


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.legacy = self.root / "legacy"
        self.target = self.root / "target"
        self.legacy.write_bytes(b"previously published binary\n")
        self.target.write_bytes(b"target binary\n")
        self.output = self.root / "package"
        self.env = {
            **os.environ,
            "JWT_VERSIONS_FILE": str(SOURCE / "jwt-plugin-versions.env"),
            "JWT_LEGACY_BINARY": str(self.legacy),
            "JWT_TARGET_BINARY": str(self.target),
            "JWT_PACKAGE_OUTPUT": str(self.output),
            "JWT_INCLUDE_LEGACY": "true",
            "JWT_PLUGIN_DIR": str(self.output / "plugins"),
            "JWT_PLUGIN_CONFIG_DIR": str(self.output / "plugin-config"),
            "JWT_PLUGIN_READY_ATTEMPTS": "3",
            "JWT_PLUGIN_READY_DELAY": "0",
            "FAKE_BAO_STATE": str(self.root / "state.json"),
            "PATH": str(self.root) + os.pathsep + os.environ["PATH"],
        }
        self.package()
        fake = self.root / "bao"
        fake.write_text(FAKE_BAO)
        fake.chmod(0o755)
        self.old_sha = hashlib.sha256(self.legacy.read_bytes()).hexdigest()
        self.target_sha = hashlib.sha256(self.target.read_bytes()).hexdigest()
        self.state = {
            "catalog": {
                "": {"command": PLUGIN, "version": "", "builtin": False, "sha256": self.old_sha},
                OLD: {"command": f"{PLUGIN}-{OLD}", "version": OLD, "builtin": False, "sha256": self.old_sha},
                TARGET: {"command": f"{PLUGIN}-{TARGET}", "version": TARGET, "builtin": False, "sha256": self.target_sha},
            },
            "mounts": {
                "services/example/jwt/": {
                    "type": PLUGIN,
                    "plugin_version": "",
                    "running_plugin_version": "",
                    "running_sha256": self.old_sha,
                },
                "auth-like-jwt/": {"type": "jwt"},
                "kv/": {"type": "kv"},
            },
            "writes": [],
        }

    def package(self):
        subprocess.run(["sh", str(SOURCE / "scripts/package-jwt-plugins.sh")], env=self.env, check=True)

    def run_driver(self, action, success=True):
        state_file = self.root / "state.json"
        state_file.write_text(json.dumps(self.state))
        result = subprocess.run(
            ["bash", str(SOURCE / "scripts/jwt-plugin-lifecycle.sh"), action],
            env=self.env, text=True, capture_output=True,
        )
        self.state = json.loads(state_file.read_text())
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def mount(self):
        return self.state["mounts"]["services/example/jwt/"]

    def reload(self):
        version = self.mount()["plugin_version"]
        self.mount()["running_plugin_version"] = version
        self.mount()["running_sha256"] = self.state["catalog"][version]["sha256"]

    def target_only(self):
        self.output = self.root / "target-only"
        self.env.update(JWT_INCLUDE_LEGACY="false", JWT_PACKAGE_OUTPUT=str(self.output),
                        JWT_PLUGIN_DIR=str(self.output / "plugins"),
                        JWT_PLUGIN_CONFIG_DIR=str(self.output / "plugin-config"))
        self.package()

    def test_bridge_preserves_bytes_and_architecture_specific_hashes(self):
        self.assertEqual((self.output / "plugins" / PLUGIN).read_bytes(), self.legacy.read_bytes())
        config = (self.output / "plugin-config/jwt.hcl").read_text()
        for version, binary in ((OLD, self.legacy), (TARGET, self.target)):
            self.assertIn(f'command = "{PLUGIN}-{version}"', config)
            self.assertIn(hashlib.sha256(binary.read_bytes()).hexdigest(), config)
        self.assertIn("plugin_auto_register = true", config)
        self.target.write_bytes(b"other architecture")
        self.output = self.root / "other-architecture"
        self.env["JWT_PACKAGE_OUTPUT"] = str(self.output)
        self.package()
        self.assertIn(hashlib.sha256(self.target.read_bytes()).hexdigest(),
                      (self.output / "plugin-config/jwt.hcl").read_text())

    def test_upgrade_rollback_and_separate_cleanup(self):
        self.run_driver("preflight")
        self.run_driver("pin-old")
        self.assertEqual(self.mount()["plugin_version"], OLD)
        self.assertEqual(self.mount()["running_plugin_version"], "")
        self.run_driver("pin-old")
        self.assertEqual(len(self.state["writes"]), 1)
        self.run_driver("upgrade", success=False)
        self.reload()
        self.run_driver("upgrade")
        self.assertEqual(self.mount()["running_plugin_version"], OLD)
        self.run_driver("check-cleanup", success=False)
        self.run_driver("rollback")
        self.assertEqual(self.mount()["plugin_version"], OLD)
        self.run_driver("upgrade")
        self.reload()
        self.run_driver("verify")
        self.run_driver("check-cleanup")
        self.run_driver("cleanup", success=False)
        self.target_only()
        self.run_driver("cleanup")
        self.assertEqual(set(self.state["catalog"]), {TARGET})
        self.assertIn("auth-like-jwt/", self.state["mounts"])

    def test_cleanup_refuses_either_old_version_and_unknown_running_digest(self):
        for configured, running in (("", ""), (OLD, TARGET), (TARGET, OLD), (TARGET, None)):
            with self.subTest(configured=configured, running=running):
                self.mount().update(plugin_version=configured, running_plugin_version=running,
                                    running_sha256=self.target_sha)
                self.run_driver("cleanup", success=False)
                self.assertEqual(self.state["writes"], [])

    def test_preflight_checks_all_mounts_before_any_tune(self):
        self.state["mounts"]["unexpected/"] = {
            "type": PLUGIN, "plugin_version": "v99.0.0",
            "running_plugin_version": "v99.0.0", "running_sha256": self.old_sha,
        }
        self.run_driver("pin-old", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_catalog_and_inventory_failures_do_not_mutate_state(self):
        self.state["catalog"][""]["sha256"] = self.target_sha
        self.run_driver("pin-old", success=False)
        self.state["fail_reads"] = True
        self.run_driver("cleanup", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_missing_or_changed_target_binary_blocks_cleanup(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        binary = self.output / "plugins" / f"{PLUGIN}-{TARGET}"
        binary.chmod(0o755)
        binary.write_bytes(b"unexpected binary")
        self.run_driver("cleanup", success=False)
        binary.unlink()
        self.run_driver("cleanup", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_legacy_plugin_mount_representation_is_included(self):
        self.mount().update(type="plugin", options={"plugin_name": PLUGIN})
        self.run_driver("pin-old")
        self.assertEqual(self.mount()["plugin_version"], OLD)

    def test_target_only_package_has_no_legacy_paths_or_stanzas(self):
        self.output = self.root / "target-only"
        self.env.update(JWT_INCLUDE_LEGACY="false", JWT_PACKAGE_OUTPUT=str(self.output))
        self.package()
        self.assertEqual([p.name for p in (self.output / "plugins").iterdir()], [f"{PLUGIN}-{TARGET}"])
        config = (self.output / "plugin-config/jwt.hcl").read_text()
        self.assertNotIn(f'version = "{OLD}"', config)

    def test_empty_and_unidentified_inventories_fail_closed(self):
        for mounts in ({}, {"kv/": {"type": "kv"}}, {"mystery/": {"type": "plugin"}}):
            with self.subTest(mounts=mounts):
                self.state["mounts"] = mounts
                for action in ("verify", "check-cleanup", "cleanup"):
                    self.run_driver(action, success=False)
                self.assertEqual(self.state["writes"], [])

    def test_deployment_can_recover_before_first_mount_but_cleanup_stays_closed(self):
        self.state["mounts"] = {"kv/": {"type": "kv"}}
        self.run_driver("deployment-preflight")
        self.run_driver("verify", success=False)
        self.run_driver("cleanup", success=False)
        self.state["catalog"][TARGET]["sha256"] = self.old_sha
        self.run_driver("deployment-preflight", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_deployment_preflight_checks_nonempty_bridge_and_target_only_states(self):
        self.run_driver("deployment-preflight")
        self.mount()["plugin_version"] = TARGET
        self.reload()
        self.target_only()
        self.run_driver("deployment-preflight")
        self.mount()["running_plugin_version"] = OLD
        self.run_driver("deployment-preflight", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_digest_identifies_a_mount_with_an_unexpected_type(self):
        self.mount()["type"] = "unexpected-alias"
        self.run_driver("cleanup", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_recovery_bridge_deployment_accepts_only_verified_post_removal_state(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        self.state["catalog"] = {TARGET: self.state["catalog"][TARGET]}
        self.run_driver("deployment-preflight")
        for configured, running, sha in ((OLD, TARGET, self.target_sha),
                                         (TARGET, OLD, self.old_sha),
                                         ("", TARGET, self.target_sha),
                                         (TARGET, TARGET, self.old_sha)):
            with self.subTest(configured=configured, running=running, sha=sha):
                self.mount().update(plugin_version=configured, running_plugin_version=running,
                                    running_sha256=sha)
                self.run_driver("deployment-preflight", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_recovery_bridge_does_not_accept_partial_or_mismatched_catalog(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        target_entry = self.state["catalog"][TARGET]
        old_entry = self.state["catalog"][OLD]
        rejected_catalogs = (
            {TARGET: target_entry, "": self.state["catalog"][""]},
            {TARGET: target_entry, OLD: {**old_entry, "sha256": self.target_sha}},
            {TARGET: {**target_entry, "sha256": self.old_sha}},
        )
        for catalog in rejected_catalogs:
            with self.subTest(catalog=catalog):
                self.state["catalog"] = catalog
                self.run_driver("deployment-preflight", success=False)
        self.state["catalog"] = {TARGET: target_entry, OLD: old_entry}
        self.run_driver("deployment-preflight")
        self.assertEqual(self.state["writes"], [])

    def test_target_only_cannot_mistake_unknown_legacy_mount_for_empty_inventory(self):
        self.target_only()
        self.mount()["type"] = "unidentified-legacy-alias"
        self.run_driver("deployment-preflight", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_bridge_preflight_checks_legacy_bytes_before_version_registration(self):
        self.state["catalog"] = {"": self.state["catalog"][""]}
        self.run_driver("bridge-preflight")
        self.state["catalog"][""]["sha256"] = self.target_sha
        self.run_driver("bridge-preflight", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_ready_check_retries_and_times_out_without_writes(self):
        self.state["not_ready_reads"] = 2
        self.run_driver("wait-ready")
        self.state["not_ready_reads"] = 3
        self.run_driver("wait-ready", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_ready_check_recovers_from_catalog_list_and_info_failures(self):
        self.state.update(catalog_list_failures=1, not_ready_reads=1)
        self.run_driver("wait-ready")
        self.assertEqual(self.state["catalog_list_failures"], 0)
        self.assertEqual(self.state["not_ready_reads"], 0)
        self.assertEqual(self.state["writes"], [])

    def test_ready_check_catalog_list_failures_have_a_bounded_timeout(self):
        self.state["catalog_list_failures"] = 5
        result = self.run_driver("wait-ready", success=False)
        self.assertEqual(self.state["catalog_list_failures"], 2)
        self.assertIn("target catalog did not become ready", result.stderr)
        self.assertEqual(self.state["writes"], [])

    def test_ready_check_rejects_invalid_or_unknown_catalog_without_retrying_info(self):
        responses = ({}, {"details": None}, {"details": [{
            "name": PLUGIN, "version": "v99.0.0", "builtin": False,
        }]})
        for response in responses:
            with self.subTest(response=response):
                self.state.update(catalog_response=response, not_ready_reads=2)
                result = self.run_driver("wait-ready", success=False)
                self.assertIn("unavailable or unknown JWT catalog versions", result.stderr)
                self.assertEqual(self.state["not_ready_reads"], 2)
                self.assertEqual(self.state["writes"], [])

    def test_cleanup_resumes_after_failure_and_handles_missing_entries(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        self.target_only()
        self.state["fail_delete_version"] = ""
        self.run_driver("cleanup", success=False)
        self.assertNotIn(OLD, self.state["catalog"])
        self.run_driver("cleanup")
        self.run_driver("cleanup")
        self.assertEqual(set(self.state["catalog"]), {TARGET})

    def test_unused_unknown_catalog_version_blocks_every_mutation(self):
        self.state["catalog"]["v99.0.0"] = {
            "version": "v99.0.0", "command": f"{PLUGIN}-v99.0.0",
            "builtin": False, "sha256": self.target_sha,
        }
        for action in ("pin-old", "upgrade", "rollback", "cleanup", "deployment-preflight"):
            self.run_driver(action, success=False)
        self.assertEqual(self.state["writes"], [])

    def test_unavailable_catalog_version_inventory_fails_closed(self):
        for response in ({}, {"details": None}, {"details": [{"name": PLUGIN}]}):
            self.state["catalog_response"] = response
            self.run_driver("deployment-preflight", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_cleanup_rejects_ha_and_unverifiable_health(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        self.target_only()
        for health in ({}, {"initialized": True, "sealed": False, "ha_enabled": True},
                       {"initialized": True, "sealed": True, "ha_enabled": False}):
            self.state["health"] = health
            self.run_driver("cleanup", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_target_only_failure_identifies_required_upgrade_stop(self):
        self.target_only()
        self.env["JWT_PLUGIN_BRIDGE_IMAGE"] = "bridge.example/release@sha256:" + "1" * 64
        result = self.run_driver("deployment-preflight", success=False)
        self.assertIn("Required upgrade stop: bridge.example/release@sha256:", result.stderr)
        self.assertIn("openbao-jwt-plugin-upgrade.md", result.stderr)
        self.assertEqual(self.state["writes"], [])

    def test_legacy_only_catalog_identifies_bridge_before_target_registration(self):
        self.target_only()
        initial_catalog = copy.deepcopy(self.state["catalog"])
        for version in ("", OLD):
            for omit_version in (False, True) if version == "" else (False,):
                with self.subTest(version=version, omit_version=omit_version):
                    entry = copy.deepcopy(initial_catalog[version])
                    if omit_version:
                        entry.pop("version")
                    self.state["catalog"] = {version: entry}
                    self.mount().update(plugin_version=version, running_plugin_version=version)
                    result = self.run_driver("deployment-preflight", success=False)
                    self.assertIn("Required upgrade stop:", result.stderr)
                    self.assertEqual(self.state["writes"], [])

    def test_target_only_read_failures_preserve_cause_without_bridge_hint(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        self.target_only()
        for operation in ("plugin list", "secrets list", "plugin info"):
            for error in ("permission denied", "connection refused", "context deadline exceeded"):
                with self.subTest(operation=operation, error=error):
                    cause = f"Injected {operation}: {error}"
                    self.state.update(fail_operation=operation, read_error=cause)
                    result = self.run_driver("deployment-preflight", success=False)
                    self.assertIn(cause, result.stderr)
                    self.assertNotIn("Required upgrade stop:", result.stderr)
                    if operation != "plugin info":
                        self.assertEqual(result.returncode, 2)
                    self.assertEqual(self.state["writes"], [])

    def test_target_only_image_config_failure_does_not_prescribe_bridge(self):
        self.target_only()
        (self.output / "plugin-config/jwt.hcl").unlink()
        result = self.run_driver("deployment-preflight", success=False)
        self.assertIn("missing declarative plugin config", result.stderr)
        self.assertNotIn("Required upgrade stop:", result.stderr)
        self.assertEqual(self.state["writes"], [])

    def test_target_only_invalid_inventory_and_catalog_do_not_prescribe_bridge(self):
        self.target_only()
        baseline = copy.deepcopy(self.state)
        cases = [
            {"raw_catalog_response": "{invalid"},
            {"catalog_response": {"details": None}},
            {"raw_mount_response": "{invalid"},
            {"catalog_response": {"details": [{"name": PLUGIN, "builtin": False, "version": "v99.0.0"}]}},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                self.state = {**copy.deepcopy(baseline), **changes}
                result = self.run_driver("deployment-preflight", success=False)
                self.assertNotIn("Required upgrade stop:", result.stderr)
                self.assertEqual(self.state["writes"], [])

    def test_unverified_mount_takes_precedence_over_another_legacy_mount(self):
        self.target_only()
        valid = {"type": PLUGIN, "plugin_version": TARGET,
                 "running_plugin_version": TARGET, "running_sha256": self.target_sha}
        cases = [{key: value for key, value in valid.items() if key != missing}
                 for missing in ("plugin_version", "running_plugin_version", "running_sha256")]
        cases += [{**valid, "plugin_version": "v99.0.0"},
                  {**valid, "running_sha256": "invalid"},
                  {**valid, "running_sha256": self.old_sha}]
        for mount in cases:
            with self.subTest(mount=mount):
                self.state["mounts"]["broken/jwt/"] = mount
                result = self.run_driver("deployment-preflight", success=False)
                self.assertNotIn("Required upgrade stop:", result.stderr)
                self.assertEqual(self.state["writes"], [])

    def test_target_only_digest_drift_does_not_prescribe_bridge(self):
        self.target_only()
        baseline = copy.deepcopy(self.state)
        for version, include_target_mount in (("", False), (TARGET, False), (TARGET, True)):
            with self.subTest(version=version, include_target_mount=include_target_mount):
                self.state = copy.deepcopy(baseline)
                self.state["catalog"][version]["sha256"] = "0" * 64
                if include_target_mount:
                    self.state["mounts"]["target/jwt/"] = {
                        "type": PLUGIN, "plugin_version": TARGET,
                        "running_plugin_version": TARGET, "running_sha256": self.target_sha}
                result = self.run_driver("deployment-preflight", success=False)
                self.assertIn("does not match the catalog", result.stderr)
                self.assertNotIn("Required upgrade stop:", result.stderr)
                self.assertEqual(self.state["writes"], [])

    def test_target_configured_old_running_requires_rotation_without_bridge_hint(self):
        self.target_only()
        for running in ("", OLD):
            with self.subTest(running=running):
                self.mount().update(plugin_version=TARGET, running_plugin_version=running)
                result = self.run_driver("deployment-preflight", success=False)
                self.assertIn("complete the checked rotation", result.stderr)
                self.assertNotIn("Required upgrade stop:", result.stderr)
                self.assertEqual(self.state["writes"], [])

    def test_target_selection_with_missing_target_catalog_is_not_legacy(self):
        self.mount()["plugin_version"] = TARGET
        self.reload()
        self.target_only()
        self.state["catalog"].pop(TARGET)
        result = self.run_driver("deployment-preflight", success=False)
        self.assertIn("invalid or missing JWT catalog entry", result.stderr)
        self.assertNotIn("Required upgrade stop:", result.stderr)
        self.assertEqual(self.state["writes"], [])

    def test_image_check_rejects_changed_command_with_unchanged_hash(self):
        config = self.output / "plugin-config/jwt.hcl"
        config.write_text(config.read_text().replace(f'command = "{PLUGIN}-{TARGET}"',
                                                    'command = "wrong-command"'))
        self.run_driver("image-check", success=False)

    def test_image_check_rejects_additional_hidden_declarations(self):
        (self.output / "plugin-config/.legacy.hcl").write_text('plugin "secret" "unexpected" {}\n')
        self.run_driver("image-check", success=False)

    def test_target_only_rejects_old_config_and_unexpected_plugin_paths(self):
        bridge_config = (self.output / "plugin-config/jwt.hcl").read_text()
        self.target_only()
        config = self.output / "plugin-config/jwt.hcl"
        target_config = config.read_text()
        config.write_text(bridge_config)
        self.run_driver("image-check", success=False)
        config.write_text(target_config)
        (self.output / "plugins" / f"{PLUGIN}-v99.0.0").write_bytes(b"unexpected")
        self.run_driver("image-check", success=False)

    def test_packaging_does_not_reuse_a_stale_layout(self):
        self.env["JWT_INCLUDE_LEGACY"] = "false"
        result = subprocess.run(["sh", str(SOURCE / "scripts/package-jwt-plugins.sh")],
                                env=self.env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be fresh", result.stderr)

    def test_image_check_needs_no_server_and_entrypoint_keeps_required_config(self):
        self.state["fail_reads"] = True
        self.run_driver("image-check")
        upstream = self.root / "upstream"
        upstream.write_text("#!/usr/bin/env python3\nimport json, sys\nprint(json.dumps(sys.argv[1:]))\n")
        upstream.chmod(0o755)
        self.env["OPENBAO_UPSTREAM_ENTRYPOINT"] = str(upstream)
        for args in (("server", "-log-level=debug"), ("bao", "server", "-config=/custom"),
                     ("bao", "version"), ("-config=/x.hcl",), ("bao", "-log-level=debug", "server")):
            result = subprocess.run(["sh", str(SOURCE / "scripts/docker-entrypoint.sh"), *args],
                                    env=self.env, capture_output=True, text=True, check=True)
            actual = json.loads(result.stdout)
            if args[0] == "server" or args[:2] == ("bao", "server"):
                self.assertIn("-config=/openbao/plugin-config", actual)
                self.assertEqual(actual[:len(args)], list(args))
            else:
                self.assertEqual(actual, list(args))


if __name__ == "__main__":
    unittest.main()
