# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Execute the NVCF 1.0.x to 1.1.0 upgrade guide commands with synthetic adapters."""

import json
import re
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from test_recovery_upgrade_check import DOCS, block_containing

UPGRADE_11 = DOCS / "overview/release-notes/1.0.1-to-1.1.0-upgrade.md"


class UpgradeGuide110Checks(unittest.TestCase):
    def test_final_diff_blocks_progress_on_changes_or_errors(self):
        for status in (0, 1, 2):
            with self.subTest(status=status):
                env = {**os.environ, "DIFF_STATUS": str(status), "CONTROL_STACK": "/target",
                       "HELMFILE_ENV": "site", "CONTROL_KUBECONFIG": "/control"}
                stub = r'''
make() { return 0; }
control_helmfile() {
  case "$1" in status) return 0 ;; diff) return "$DIFF_STATUS" ;; *) return 99 ;; esac
}
'''
                command = block_containing(UPGRADE_11, 'control_helmfile status')
                result = subprocess.run(["bash", "-euo", "pipefail", "-c",
                                         stub + command + '\necho resume-traffic'],
                                        env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertEqual('resume-traffic' in result.stdout, status == 0)

    def test_missing_artifacts_stop_after_reporting_all_checks(self):
        script = r'''
helm() { test "$MISSING_KIND" != chart; }
docker() { test "$MISSING_KIND" != image; }
'''
        command = block_containing(UPGRADE_11, 'artifact_missing=0')
        for kind in ("none", "chart", "image"):
            with self.subTest(kind=kind):
                result = subprocess.run(["bash", "-euo", "pipefail", "-c",
                                         script + command + "\necho CONTINUED"],
                                        env={**os.environ, "MISSING_KIND": kind}, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, kind == "none", result.stderr)
                self.assertEqual("CONTINUED" in result.stdout, kind == "none")
                if kind != "none":
                    self.assertIn("MISSING", result.stdout)

    def test_starting_state_requires_ready_pods_and_rejects_empty_namespaces(self):
        ready = {"metadata": {}, "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
        completed = {"metadata": {}, "status": {"phase": "Succeeded"}}
        cases = [(True, [ready]), (True, [ready, completed]), (False, []),
                 (False, [{"metadata": {}, "status": {"phase": "Pending"}}]),
                 (False, [ready, {"metadata": {}, "status": {"phase": "Failed"}}]),
                 (False, [{**ready, "metadata": {"deletionTimestamp": "2026-01-01"}}])]
        command = block_containing(UPGRADE_11, "# Releases deployed")
        stubs = r'''
helm() { printf '%s\n' '[{"name":"api","status":"deployed"}]'; }
python3() { return 0; }
kc() {
  case "$*" in
    *'get pods'*'jsonpath'*) printf '%s\n' cassandra-0 ;;
    *'get pods'*'-o json'*) printf '%s\n' "$PODS" ;;
  esac
}
'''
        for expected, pods in cases:
            with self.subTest(pods=pods), tempfile.TemporaryDirectory() as directory:
                result = subprocess.run(["bash", "-euo", "pipefail", "-c", stubs + command + "\necho CONTINUED"],
                                        env={**os.environ, "PODS": json.dumps({"items": pods}),
                                             "WORKDIR": directory, "CONTROL_KUBECONFIG": "synthetic",
                                             "UPGRADE_SCRIPTS": directory},
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, expected, result.stderr)
                self.assertEqual("CONTINUED" in result.stdout, expected)

    def test_identity_discovery_precedes_registration_file_gate(self):
        command = block_containing(UPGRADE_11, "record_identity() {")
        stubs = r'''
kubectl() {
  case "$*" in
    *configmap*) printf '%s\n' '{"clusterId":"id","clusterGroupId":"group","clusterName":"actual-name","ncaID":"account"}' ;;
  esac
}
'''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", stubs + command.replace("/secure/path/to/existing-registration", str(Path(directory) / "missing"))],
                                    env={**os.environ, "WORKDIR": directory}, capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("actual-name", result.stdout)
            self.assertFalse((Path(directory) / "missing").exists())


class CassandraReplicationCommands(unittest.TestCase):
    def run_replication(self, configured="custom_dc", live="custom_dc"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "cqlsh"
            binary.write_text('''#!/usr/bin/env python3
import json, os, pathlib, stat, sys
args = sys.argv[1:]
credentials = pathlib.Path(next(a.split('=', 1)[1] for a in args if a.startswith('--credentials=')))
assert stat.S_IMODE(credentials.stat().st_mode) == 0o600
assert 'password = fixture-secret' in credentials.read_text()
assert all('fixture-secret' not in a for a in args)
with open(os.environ['CQL_LOG'], 'a') as log: log.write(json.dumps(args) + '\\n')
query = args[-1]
if 'SELECT JSON data_center' in query: print(json.dumps({'data_center': os.environ['LIVE_DC']}))
elif 'SELECT keyspace_name FROM' in query: print('nvcf_api')
''')
            binary.chmod(0o755)
            stubs = r'''
kc() {
  case "$*" in
    *'get statefulset'*) printf '%s\n' "$STATEFULSET" ;;
    *'exec '*' -- sh '*)
      while [ "$1" != -- ]; do shift; done
      shift
      "$@" ;;
    *'nodetool repair'*) echo repair >> "$CQL_LOG" ;;
    *) return 99 ;;
  esac
}
'''
            statefulset = {"spec": {"template": {"spec": {"containers": [{"name": "cassandra", "env": [{"name": "CASSANDRA_DC", "value": configured}]}]}}}}
            env = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"], "TMPDIR": directory,
                   "CQL_LOG": str(root / "calls"), "LIVE_DC": live, "STATEFULSET": json.dumps(statefulset)}
            command = block_containing(UPGRADE_11, "LIVE_DC=$(cql")
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", stubs + command + "\necho CONTINUED"],
                                    env=env, input="fixture-secret\n", capture_output=True, text=True)
            calls = (root / "calls").read_text() if (root / "calls").exists() else ""
            leftovers = [path for path in root.iterdir() if path.is_dir()]
            return result, calls, leftovers

    def test_custom_live_datacenter_is_used_and_password_stays_out_of_arguments(self):
        result, calls, leftovers = self.run_replication()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("'custom_dc': '3'", calls)
        self.assertNotIn("'ncp': '3'", calls)
        self.assertNotIn("fixture-secret", calls + result.stdout + result.stderr)
        self.assertEqual(3, calls.count("repair\n"))
        self.assertEqual([], leftovers)

    def test_datacenter_mismatch_or_unsafe_name_stops_before_alter_and_repair(self):
        for configured, live in (("custom_dc", "other"), ("bad'name", "bad'name")):
            with self.subTest(configured=configured):
                result, calls, leftovers = self.run_replication(configured, live)
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("ALTER", calls)
                self.assertNotIn("repair", calls)
                self.assertNotIn("CONTINUED", result.stdout)
                self.assertEqual([], leftovers)


class CassandraExpansionPreparation(unittest.TestCase):
    STUBS = r"""
kc() {
  case "$*" in
    *'get statefulset'*'jsonpath'*) printf '%s members now\n' "${LIVE_REPLICAS:-1}" ;;
    *'get statefulset'*) printf '%s\n' "$STATEFULSET" ;;
    *'logs deploy/nvcf-api'*) if [ -n "$LOGS_FAIL" ]; then return 1; fi; printf '%s\n' "$LOGS_TEXT" ;;
    *'patch statefulset'*) echo "patch $*" >> "$CQL_LOG" ;;
    *'get nodes'*) echo "nodes $*" >> "$CQL_LOG" ;;
    *'scale statefulset'*) echo "scale $*" >> "$CQL_LOG" ;;
    *'rollout status'*) echo "rollout $*" >> "$CQL_LOG" ;;
    *'exec -i '*' -- sh '*)
      prev=; POD=; for a in "$@"; do if [ "$prev" = -i ]; then POD=$a; break; fi; prev=$a; done; export POD
      while [ "$1" != -- ]; do shift; done
      shift
      "$@" ;;
    *'nodetool repair'*) echo "repair $*" >> "$CQL_LOG" ;;
    *'nodetool status'*) echo "status $*" >> "$CQL_LOG" ;;
    *) return 99 ;;
  esac
}
yq() { printf '%s\n' "$HA_MODE_STUB"; }
"""

    CQLSH = r"""#!/usr/bin/env python3
import json, os, pathlib, stat, sys
args = sys.argv[1:]
credentials = pathlib.Path(next(a.split('=', 1)[1] for a in args if a.startswith('--credentials=')))
assert stat.S_IMODE(credentials.stat().st_mode) == 0o600
assert 'password = fixture-secret' in credentials.read_text()
assert all('fixture-secret' not in a for a in args)
query = args[-1]
with open(os.environ['CQL_LOG'], 'a') as log: log.write(json.dumps([os.environ.get('POD'), query]) + '\n')
if 'system_auth.roles' in query and os.environ.get('FAIL_POD') == os.environ.get('POD'): sys.exit(1)
if 'SELECT keyspace_name FROM' in query: print('nvcf_api')
"""

    def run_block(self, marker_blocks, replicas="1", fail_pod="", datacenter="custom_dc",
                  ha_mode="preferred", logs_fail="", logs_text="", saved_replicas="same"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cqlsh").write_text(self.CQLSH)
            (root / "cqlsh").chmod(0o755)
            statefulset = {"spec": {"template": {"spec": {"containers": [
                {"name": "cassandra", "env": [{"name": "CASSANDRA_DC", "value": datacenter}]}]}}}}
            backup = root / "backup"
            backup.mkdir()
            if saved_replicas is not None:
                (backup / "cassandra-replicas").write_text((replicas if saved_replicas == "same" else saved_replicas) + "\n")
            env = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"], "TMPDIR": directory,
                   "CQL_LOG": str(root / "calls"), "STATEFULSET": json.dumps(statefulset),
                   "FAIL_POD": fail_pod, "BACKUP_DIR": str(backup), "CONTROL_STACK": "/stack",
                   "HELMFILE_ENV": "env", "HA_MODE_STUB": ha_mode, "LOGS_FAIL": logs_fail, "LOGS_TEXT": logs_text}
            env.pop("CASSANDRA_REPLICAS", None)
            commands = "\n".join(block_containing(UPGRADE_11, marker) for marker in marker_blocks)
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", self.STUBS + commands + "\necho CONTINUED"],
                                    env=env, input="fixture-secret\n", capture_output=True, text=True)
            calls = (root / "calls").read_text() if (root / "calls").exists() else ""
            return result, calls

    SCALE = "scale statefulset cassandra"
    AUTH = 'cql_on cassandra-0 "ALTER KEYSPACE system_auth'
    APPS = 'cql_on cassandra-0 "SELECT keyspace_name FROM system_schema.keyspaces;"'

    def test_zone_spread_rule_is_hard_under_enforced_and_soft_under_preferred(self):
        for mode, expected in (("enforced", "DoNotSchedule"), ("preferred", "ScheduleAnyway")):
            with self.subTest(mode=mode):
                result, calls = self.run_block(["topologySpreadConstraints"], ha_mode=mode)
                self.assertEqual(0, result.returncode, result.stderr)
                patch = next(line for line in calls.splitlines() if line.startswith("patch"))
                body = json.loads(re.search(r" -p (\{.*\})$", patch).group(1))
                rule = body["spec"]["template"]["spec"]["topologySpreadConstraints"]
                self.assertEqual(1, len(rule))
                self.assertEqual("topology.kubernetes.io/zone", rule[0]["topologyKey"])
                self.assertEqual(expected, rule[0]["whenUnsatisfiable"])
                self.assertEqual(1, rule[0]["maxSkew"])
                self.assertEqual({"app.kubernetes.io/name": "cassandra"}, rule[0]["labelSelector"]["matchLabels"])
                self.assertIn("rollout", calls)

    def test_zone_spread_rule_stops_on_an_unknown_mode(self):
        result, calls = self.run_block(["topologySpreadConstraints"], ha_mode="none")
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("patch", calls)
        self.assertNotIn("CONTINUED", result.stdout)

    def test_scale_runs_only_for_a_single_member(self):
        result, calls = self.run_block([self.SCALE], replicas="1")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--replicas=3", calls)
        self.assertIn("rollout", calls)
        result, calls = self.run_block([self.SCALE], replicas="3")
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("scale", calls)
        self.assertNotIn("CONTINUED", result.stdout)

    def test_scale_restores_the_saved_count_and_stops_without_it(self):
        result, calls = self.run_block([self.SCALE], replicas="1")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("members now", result.stdout + calls + result.stderr)
        result, calls = self.run_block([self.SCALE], saved_replicas=None)
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("scale", calls)

    LOGS = "AUTH_LOGS=$(kc -n nvcf logs"

    def test_log_check_counts_errors_and_stops_when_the_logs_cannot_be_read(self):
        result, calls = self.run_block([self.LOGS], logs_text="ok\nAuthenticationException: bad\nAuthenticationException: bad")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("2", result.stdout.split()[0])
        result, calls = self.run_block([self.LOGS], logs_text="all quiet")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("0", result.stdout.split()[0])
        result, calls = self.run_block([self.LOGS], logs_fail="1")
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("CONTINUED", result.stdout)
        self.assertNotIn("0", result.stdout.split())

    def test_system_auth_is_raised_repaired_and_checked_on_every_member(self):
        result, calls = self.run_block([self.AUTH])
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("'custom_dc': '3'", calls)
        self.assertNotIn("'ncp': '3'", calls)
        for pod in ("cassandra-0", "cassandra-1", "cassandra-2"):
            self.assertIn(f"exec {pod} -c cassandra -- nodetool repair --full system_auth", calls)
            self.assertIn(f'["{pod}", "SELECT role FROM system_auth.roles LIMIT 1;"]', calls)
            self.assertIn(f"administrator login ok on {pod}", result.stdout)
        self.assertNotIn("fixture-secret", calls + result.stdout + result.stderr)

    def test_a_failed_login_on_any_member_stops_the_procedure(self):
        result, calls = self.run_block([self.AUTH], fail_pod="cassandra-1")
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("CONTINUED", result.stdout)
        self.assertNotIn("login ok on cassandra-2", result.stdout)

    def test_application_keyspaces_are_raised_only_when_present_then_repaired(self):
        result, calls = self.run_block([self.AUTH, self.APPS])
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("ALTER KEYSPACE nvcf_api", calls)
        self.assertNotIn("ALTER KEYSPACE nvct_api", calls)
        self.assertIn("skipping nvct_api: not present", result.stdout)
        self.assertEqual(3, calls.count("nodetool repair --full\n"))
        self.assertNotIn("fixture-secret", calls + result.stdout + result.stderr)


class Receipt11Checks(unittest.TestCase):
    def run_checks(self, control_stamp, compute_stamp, receipt, after_preflight=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control, compute = root / "control", root / "compute"
            for stack, stamp in ((control, control_stamp), (compute, compute_stamp)):
                stack.mkdir()
                if stamp is not None:
                    (stack / "VERSION").write_text(stamp)
            preflight = block_containing(UPGRADE_11, "check_selected_stack_versions() {")
            final = block_containing(UPGRADE_11, "get job upgrade-receipt")
            script = r'''
set -euo pipefail
control_helmfile() { printf 'APPLIED\n'; }
kc() { case "$*" in *jsonpath*) printf '%s' "$RECEIPT_VERSION" ;; esac; }
'''
            script += preflight + "\n" + after_preflight + "\n" + final + "\nprintf 'CONTINUED\\n'"
            env = {**os.environ, "CONTROL_STACK": str(control), "COMPUTE_STACK": str(compute),
                   "STACK_VERSION": "1.1.2", "COMPUTE_STACK_VERSION": "1.1.0",
                   "WORKDIR": str(root), "RECEIPT_VERSION": receipt}
            return subprocess.run(["bash", "-c", script], env=env,
                                  capture_output=True, text=True)

    def test_correct_selection_passes(self):
        result = self.run_checks("1.1.2", "1.1.0", "1.1.2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CONTINUED", result.stdout)

    def test_missing_wrong_or_shared_wrong_versions_stop(self):
        for control, compute, receipt in ((None, "1.1.0", "1.1.2"),
                                          ("1.1.2", "", "1.1.2"),
                                          ("1.1.0", "1.1.0", "1.1.0"),
                                          ("1.1.2", "1.1.0", "1.1.0")):
            with self.subTest(control=control, compute=compute, receipt=receipt):
                result = self.run_checks(control, compute, receipt)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("CONTINUED", result.stdout)
                if control != "1.1.2" or compute != "1.1.0":
                    self.assertNotIn("APPLIED", result.stdout)

    def test_changed_package_stops_before_receipt_apply(self):
        result = self.run_checks("1.1.2", "1.1.0", "1.1.2",
                                 'printf "1.1.0\\n" > "$CONTROL_STACK/VERSION"')
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("APPLIED", result.stdout)


if __name__ == "__main__":
    unittest.main()
