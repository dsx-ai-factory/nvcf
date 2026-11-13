# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[3]
TOOL = REPO / "deploy/helm/openbao/helm/scripts/jwt-plugin-cluster.sh"
VERSIONS = dict(line.split("=", 1) for line in
                (REPO / "infra/openbao/jwt-plugin-versions.env").read_text().splitlines()
                if line and not line.startswith("#"))
DIGEST = "sha256:" + "1" * 64
TOKEN = "operator-credential-never-in-argv"

FAKE_KUBECTL = r'''#!/usr/bin/env python3
import base64, fcntl, json, os, sys
from pathlib import Path
a = sys.argv[1:]
received_token = sys.stdin.readline().strip() if "exec" in a else None
p = Path(os.environ["FAKE_KUBE_STATE"])
lock = open(str(p) + ".lock", "w")
fcntl.flock(lock, fcntl.LOCK_EX)
s = json.loads(p.read_text())
if "exec" in a:
    member = a[a.index("exec") + 2]
    remote = a[a.index("--") + 1:]
    op = remote[4]
    if s["token"] in " ".join(a) or received_token != s["token"]:
        sys.exit("credential transport failure")
    s["calls"].append(["exec", member, op])
    if member in s.get("bad_members", []):
        sys.exit(1)
    if op == "info":
        result = {"metadata": s["metadata"], "node_id": "node-" + member, "active": s["active"] == member}
    elif op == "snapshot":
        if s["active"] != member:
            sys.exit("standby cannot record active evidence")
        result = {"metadata": s["metadata"], "mounts": s["mounts"], "raft_peers": s["peers"]}
    elif op == "catalog":
        result = {"details": list(s["catalog"].values())}
    elif op == "retire":
        version = remote[5]
        s["writes"].append(version)
        if s.get("fail_delete") == version:
            s.pop("fail_delete")
            p.write_text(json.dumps(s))
            sys.exit(1)
        s["catalog"].pop(version, None)
        result = None
    else:
        sys.exit("unknown operation: " + op)
else:
    kind = a[a.index("get") + 1]
    s["calls"].append(["get", kind])
    if s.get("fail_get") == kind:
        sys.exit(1)
    if kind == "statefulset":
        result = s["statefulset"]
    elif kind == "pods":
        result = {"items": s["pods"]}
    elif kind == "secret":
        result = {"data": {"root_token": base64.b64encode(s["token"].encode()).decode()}}
    else:
        sys.exit("unknown resource")
p.write_text(json.dumps(s))
if result is not None:
    print(json.dumps(result))
'''


class ClusterCleanupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state_file = self.root / "state.json"
        self.evidence = self.root / "evidence.json"
        self.expected_digest = DIGEST
        self.image_manifest = None
        self.names = [f"renamed-bao-{i}" for i in range(3)]
        labels = {"app": "renamed-bao"}
        self.state = {
            "token": TOKEN, "calls": [], "writes": [], "active": self.names[0],
            "metadata": {"cluster_id": "original-cluster", "sha": "2" * 64,
                         "old": VERSIONS["JWT_PLUGIN_OLD_VERSION"],
                         "target": VERSIONS["JWT_PLUGIN_TARGET_VERSION"]},
            "mounts": [{"key": "original/jwt/", "accessor": "original-accessor",
                        "config": {"default_lease_ttl": 0}, "options": {},
                        "configured": VERSIONS["JWT_PLUGIN_TARGET_VERSION"],
                        "running": VERSIONS["JWT_PLUGIN_TARGET_VERSION"], "sha": "2" * 64}],
            "statefulset": {"metadata": {"uid": "sts-uid", "generation": 2},
                            "spec": {"replicas": 3, "selector": {"matchLabels": labels}}},
            "pods": [], "catalog": {},
            "peers": [{"node_id": "node-" + name, "address": name + ":8201", "voter": True}
                      for name in self.names],
        }
        for name in self.names:
            self.state["pods"].append({
                "metadata": {"name": name, "uid": name + "-uid", "labels": labels,
                             "ownerReferences": [{"uid": "sts-uid", "controller": True}]},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}],
                           "containerStatuses": [{"name": "openbao", "ready": True,
                                                  "containerID": "containerd://" + name,
                                                  "restartCount": 0, "imageID": "registry/image@" + DIGEST,
                                                  "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}}]}})
        for version in ("", VERSIONS["JWT_PLUGIN_OLD_VERSION"], VERSIONS["JWT_PLUGIN_TARGET_VERSION"]):
            self.state["catalog"][version] = {"name": "vault-plugin-secrets-jwt", "version": version}
        fake = self.root / "kubectl"
        fake.write_text(FAKE_KUBECTL)
        fake.chmod(0o755)
        self.probe = self.root / "probe"
        self.probe.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
p = Path(os.environ["FAKE_KUBE_STATE"])
s = json.loads(p.read_text())
s["probed_pod"] = os.environ["JWT_PROBE_POD"]
if s.get("probe_drift"):
    s["pods"][0]["status"]["containerStatuses"][0]["restartCount"] += 1
if s.get("probe_mount_drift"):
    s["mounts"][0]["config"]["default_lease_ttl"] = 30
p.write_text(json.dumps(s))
sys.exit(s.get("probe_exit", 0))
''')
        self.probe.chmod(0o755)
        self.env = {**os.environ, "FAKE_KUBE_STATE": str(self.state_file),
                    "PATH": str(self.root) + os.pathsep + os.environ["PATH"]}

    def run_tool(self, action, *options, success=True):
        self.state_file.write_text(json.dumps(self.state))
        args = ["bash", str(TOOL), action,
                                 "--context", "isolated-context", "--namespace", "tenant",
                                 "--statefulset", "renamed-bao", "--expected-image-digest", self.expected_digest,
                                 "--maintenance-window", "window-1", "--evidence", str(self.evidence),
                                 *options]
        if self.image_manifest:
            args.extend(["--image-manifest", str(self.image_manifest)])
        result = subprocess.run(args, env=self.env, capture_output=True, text=True)
        self.state = json.loads(self.state_file.read_text())
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)
        return result

    def record(self, member, success=True):
        return self.run_tool("record-active", "--pod", member, "--probe", str(self.probe), success=success)

    def collect(self):
        for member in self.names[:2]:
            self.state["active"] = member
            self.record(member)

    def test_cleanup_requires_normal_failover_and_preserves_target(self):
        self.record(self.names[0])
        self.run_tool("check-cleanup", success=False)
        self.assertEqual(self.state["writes"], [])
        self.collect()
        self.assertEqual(set(json.loads(self.evidence.read_text())["records"]), set(self.names[:2]))
        self.run_tool("check-cleanup")
        self.run_tool("cleanup", "--confirm-rollback-window-closed")
        self.run_tool("cleanup", "--confirm-rollback-window-closed")
        self.assertEqual(self.state["writes"], [VERSIONS["JWT_PLUGIN_OLD_VERSION"], ""])
        self.assertEqual(set(self.state["catalog"]), {VERSIONS["JWT_PLUGIN_TARGET_VERSION"]})

    def test_repeated_same_active_does_not_prove_failover(self):
        self.record(self.names[0])
        self.record(self.names[0])
        self.run_tool("check-cleanup", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_one_voter_raft_cluster_accepts_one_active_record(self):
        self.state["statefulset"]["spec"]["replicas"] = 1
        self.state["pods"] = self.state["pods"][:1]
        self.state["peers"] = self.state["peers"][:1]
        self.record(self.names[0])
        self.run_tool("check-cleanup")
        self.run_tool("cleanup", "--confirm-rollback-window-closed")
        self.assertEqual(set(self.state["catalog"]), {VERSIONS["JWT_PLUGIN_TARGET_VERSION"]})

    def test_foreign_active_record_is_rejected(self):
        self.collect()
        receipt = json.loads(self.evidence.read_text())
        receipt["records"]["unmanaged"] = receipt["records"][self.names[0]]
        self.evidence.write_text(json.dumps(receipt))
        self.run_tool("cleanup", "--confirm-rollback-window-closed", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_old_evidence_schema_requires_new_validation(self):
        self.collect()
        receipt = json.loads(self.evidence.read_text())
        receipt["schema"] = 1
        self.evidence.write_text(json.dumps(receipt))
        self.run_tool("check-cleanup", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_standby_and_failed_probe_do_not_record_evidence(self):
        self.record(self.names[1], success=False)
        self.state["probe_exit"] = 1
        self.record(self.names[0], success=False)
        self.assertFalse(self.evidence.exists())
        self.assertEqual(self.state["writes"], [])

    def test_probe_restart_invalidates_recording(self):
        self.state["probe_drift"] = True
        self.record(self.names[0], success=False)
        self.assertFalse(self.evidence.exists())

    def test_probe_mount_tune_invalidates_recording(self):
        self.state["probe_mount_drift"] = True
        self.record(self.names[0], success=False)
        self.assertFalse(self.evidence.exists())

    def test_missing_unready_foreign_and_wrong_image_members_block(self):
        original = json.dumps(self.state["pods"])
        mutations = [lambda pods: pods.pop(),
                     lambda pods: pods[0]["status"]["conditions"][0].update(status="False"),
                     lambda pods: pods[0]["metadata"]["ownerReferences"][0].update(uid="other"),
                     lambda pods: pods[0]["metadata"].update(deletionTimestamp="now"),
                     lambda pods: pods[0]["status"]["containerStatuses"][0].update(imageID="wrong")]
        for mutate in mutations:
            self.state["pods"] = json.loads(original)
            mutate(self.state["pods"])
            self.record(self.names[0], success=False)
        self.assertFalse(self.evidence.exists())
        self.assertEqual(self.state["writes"], [])

    def test_unknown_raft_peer_blocks_active_evidence(self):
        self.state["peers"].append({"node_id": "unmanaged", "address": "unknown:8201", "voter": True})
        self.record(self.names[0], success=False)
        self.assertFalse(self.evidence.exists())

    def test_changed_pod_container_or_mounts_invalidates_evidence(self):
        self.collect()
        original = json.dumps(self.state)
        mutations = [lambda s: s["pods"][0]["metadata"].update(uid="new-pod"),
                     lambda s: s["pods"][0]["status"]["containerStatuses"][0].update(containerID="new-container"),
                     lambda s: s["mounts"][0].update(accessor="recreated-mount"),
                     lambda s: s["mounts"][0]["config"].update(default_lease_ttl=30),
                     lambda s: s["metadata"].update(cluster_id="different-cluster")]
        for mutate in mutations:
            self.state = json.loads(original)
            mutate(self.state)
            self.run_tool("cleanup", "--confirm-rollback-window-closed", success=False)
            self.assertEqual(self.state["writes"], [])

    def test_inventory_reads_and_incompatible_member_fail_before_writes(self):
        self.collect()
        for kind in ("pods", "statefulset", "secret"):
            self.state["fail_get"] = kind
            self.run_tool("cleanup", "--confirm-rollback-window-closed", success=False)
        self.state.pop("fail_get")
        self.state["bad_members"] = [self.names[1]]
        self.run_tool("cleanup", "--confirm-rollback-window-closed", success=False)
        self.assertEqual(self.state["writes"], [])

    def test_partial_cleanup_can_resume_without_recreating_entries(self):
        self.collect()
        self.state["fail_delete"] = ""
        self.run_tool("cleanup", "--confirm-rollback-window-closed", success=False)
        self.assertNotIn(VERSIONS["JWT_PLUGIN_OLD_VERSION"], self.state["catalog"])
        self.run_tool("cleanup", "--confirm-rollback-window-closed")
        self.assertEqual(set(self.state["catalog"]), {VERSIONS["JWT_PLUGIN_TARGET_VERSION"]})

    def test_cleanup_requires_rollback_window_exit(self):
        result = self.run_tool("cleanup", success=False)
        self.assertIn("--confirm-rollback-window-closed", result.stderr)
        self.assertEqual(self.state["calls"], [])

    def test_config_only_runtime_ids_require_a_verified_platform_manifest(self):
        config = "sha256:" + "3" * 64
        for pod in self.state["pods"]:
            pod["status"]["containerStatuses"][0]["imageID"] = config
        self.record(self.names[0], success=False)
        self.expected_digest = config
        self.record(self.names[0], success=False)
        self.image_manifest = self.root / "platform.json"
        self.image_manifest.write_text(json.dumps({
            "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"digest": config}, "layers": [],
        }) + "\n\n")
        self.expected_digest = "sha256:" + hashlib.sha256(self.image_manifest.read_bytes()).hexdigest()
        self.collect()
        self.run_tool("cleanup", "--confirm-rollback-window-closed")
        self.assertEqual(set(self.state["catalog"]), {VERSIONS["JWT_PLUGIN_TARGET_VERSION"]})

    def test_changed_platform_manifest_is_rejected_before_cluster_reads(self):
        self.image_manifest = self.root / "platform.json"
        self.image_manifest.write_text('{"config":{"digest":"sha256:' + "3" * 64 + '"}}')
        self.record(self.names[0], success=False)
        self.assertEqual(self.state["calls"], [])


if __name__ == "__main__":
    unittest.main()
