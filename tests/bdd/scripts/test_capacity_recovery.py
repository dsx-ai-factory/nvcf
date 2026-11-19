# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from check_capacity_recovery import (
    allocation_released,
    discovery_mode,
    latest_heartbeat,
    occupied_by_candidate,
    periodic_heartbeat_seconds,
    ready_workers,
    RecoveryCheck,
)


class CapacityRecoveryTests(unittest.TestCase):
    def test_failure_deletes_owned_definitions_and_continues_after_cleanup_error(self):
        with tempfile.TemporaryDirectory() as output:
            check = RecoveryCheck.__new__(RecoveryCheck)
            check.args = SimpleNamespace(expected_capacity=2)
            check.output = Path(output)
            check.identities = [("owned-first", "version-one"), ("owned-second", "version-two")]
            check.wait = Mock(side_effect=RuntimeError("primary failure"))
            check.cli = Mock(side_effect=[RuntimeError("delete failed"), "deleted"])
            with self.assertRaisesRegex(RuntimeError, "primary failure"):
                check.run()
            self.assertEqual([
                ("function", "delete", "--function-id", "owned-second", "--version-id", "version-two"),
                ("function", "delete", "--function-id", "owned-first", "--version-id", "version-one"),
            ], [call.args for call in check.cli.call_args_list])
            self.assertEqual({"failedFunctionDeletions": 1},
                             json.loads((check.output / "cleanup.json").read_text()))

    def test_release_requires_object_gone_and_termination_reported(self):
        for exists in (False, True):
            for reported in (False, True):
                with self.subTest(exists=exists, reported=reported):
                    pods = [{"metadata": {"name": "old"}}] if exists else []
                    requests = [{"status": {"instances": {"old": {
                        "lastReportedStatus": "terminated" if reported else "running"}}}}]
                    self.assertEqual(not exists and reported,
                                     allocation_released("old", pods, requests))
        self.assertTrue(allocation_released("old", [], []))
        self.assertFalse(allocation_released("old", [], [
            {"status": {"instances": {"different-map-key": {"id": "old"}}}}]))

    def test_occupied_check_rejects_wrong_agent_and_spare_capacity(self):
        usage = {"capacity": 4, "allocated": 4, "available": 0}
        heartbeat = {"nvcaAgentVersion": "candidate", "gpuUsage": {"H100": {"allocated": 4}}}
        check = lambda u, h: occupied_by_candidate(u, h, "H100", 4, "candidate")
        self.assertTrue(check(usage, heartbeat))
        self.assertFalse(check(usage, {**heartbeat, "nvcaAgentVersion": "released"}))
        self.assertFalse(check({**usage, "available": 1}, heartbeat))
        self.assertFalse(check({**usage, "allocated": 3}, heartbeat))
        self.assertFalse(check(usage, {**heartbeat, "gpuUsage": {}}))
        self.assertFalse(check(usage, None))

    def test_heartbeat_parser_uses_latest_complete_payload(self):
        first = {"nvcaAgentVersion": "released"}
        last = {"nvcaAgentVersion": "candidate"}
        logs = ("unrelated\nheartBeatRequest " + json.dumps(first) + " suffix\n"
                + "heartBeatRequest " + json.dumps(last).replace('"', '\\"') + " suffix\n"
                + "heartBeatRequest invalid")
        self.assertEqual(last, latest_heartbeat(logs))
        self.assertIsNone(latest_heartbeat("no heartbeat"))

    def test_heartbeat_interval_is_observed_in_startup(self):
        self.assertEqual(300, periodic_heartbeat_seconds("options: HeartbeatInterval:300000000000, rest"))
        self.assertEqual(15, periodic_heartbeat_seconds("options: HeartbeatInterval:15000000000, rest"))
        with self.assertRaises(RuntimeError):
            periodic_heartbeat_seconds("no effective interval")

    def test_discovery_mode_is_observed_in_startup(self):
        self.assertEqual("dynamic", discovery_mode("DynamicGPUDiscoveryEnabled:true,"))
        self.assertEqual("static", discovery_mode("DynamicGPUDiscoveryEnabled:false,"))
        with self.assertRaises(RuntimeError):
            discovery_mode("mode not reported")

    def test_only_ready_non_deleting_workers_for_the_target_functions_count(self):
        def pod(function_id, ready=True, deleting=False):
            metadata = {"labels": {"function-id": function_id}}
            if deleting:
                metadata["deletionTimestamp"] = "now"
            return {"metadata": metadata, "status": {"conditions": [
                {"type": "Ready", "status": "True" if ready else "False"}]}}
        pods = [pod("target"), pod("other"), pod("target", False), pod("target", deleting=True)]
        self.assertEqual([pods[0]], ready_workers(pods, {"target"}))


if __name__ == "__main__":
    unittest.main()
