# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from check_capacity_recovery import (
    allocation_released,
    capacity_publication_observed,
    discovery_mode,
    latest_heartbeat,
    occupied_by_candidate,
    periodic_heartbeat_seconds,
    prepared_discovery,
    ready_workers,
    RecoveryCheck,
)


class CapacityRecoveryTests(unittest.TestCase):
    def test_only_successful_capacity_triggered_publication_counts(self):
        self.assertTrue(capacity_publication_observed('level=info msg="Published NVCA heartbeat" heartbeat_trigger=capacity'))
        self.assertTrue(capacity_publication_observed('{"msg":"Published NVCA heartbeat","heartbeat_trigger":"capacity"}'))
        self.assertFalse(capacity_publication_observed('level=info msg="Published NVCA heartbeat" heartbeat_trigger=periodic'))
        self.assertFalse(capacity_publication_observed('level=error msg="request failed" heartbeat_trigger=capacity'))

    def test_created_identity_is_owned_before_deployment_or_cli_state_can_fail(self):
        check = RecoveryCheck.__new__(RecoveryCheck)
        check.args = SimpleNamespace(image="sample", backend="target", gpu="H100", instance_type="one", region="local")
        check.identities = []
        check.save = Mock()
        check.cli = Mock(side_effect=[json.dumps({"function": {"id": "owned", "versionId": "version"}}),
                                      RuntimeError("deployment failed")])
        with self.assertRaisesRegex(RuntimeError, "deployment failed"):
            check.create("unique-name")
        self.assertEqual([("owned", "version")], check.identities)
        self.assertEqual("--json", check.cli.call_args_list[0].args[-1])
        self.assertFalse(any(call.args[0] == "status" for call in check.cli.call_args_list))

    def test_wait_rejects_a_successful_observation_completed_after_its_deadline(self):
        for duration, timeout, accepted in ((0.5, 1, True), (1.5, 1, False)):
            with self.subTest(duration=duration):
                clock = [0]
                check = RecoveryCheck.__new__(RecoveryCheck)

                def condition():
                    clock[0] += duration
                    return "observed"

                with patch("check_capacity_recovery.time.monotonic", side_effect=lambda: clock[0]):
                    if accepted:
                        self.assertEqual("observed", check.wait("bounded", condition, timeout))
                    else:
                        with self.assertRaisesRegex(RuntimeError, "bounded timed out"):
                            check.wait("bounded", condition, timeout)

    def test_wait_rejects_periodic_fallback_even_within_the_release_relative_limit(self):
        clock = [0]
        check = RecoveryCheck.__new__(RecoveryCheck)

        def condition():
            clock[0] = 51
            return True

        with patch("check_capacity_recovery.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(RuntimeError, "before periodic timed out"):
                check.wait("before periodic", condition, 60, deadline=50)

    def test_command_and_http_calls_use_the_remaining_observation_budget(self):
        check = RecoveryCheck.__new__(RecoveryCheck)
        check.deadline = 10
        check.args = SimpleNamespace(api_url="http://local", account="test", icms_host="sis", backend="target", gpu="H100")
        check.token = "unit-token"
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = json.dumps({"clusters": [{"clusterName": "target", "gpuUsage": {"H100": {}}}]}).encode()
        check.http = Mock()
        check.http.open.return_value = response
        with patch("check_capacity_recovery.time.monotonic", return_value=9), \
                patch("check_capacity_recovery.subprocess.run", return_value=SimpleNamespace(returncode=0, stdout="ok")) as run:
            self.assertEqual("ok", check.command(["tool"]))
            self.assertEqual(1, run.call_args.kwargs["timeout"])
            check.usage()
            self.assertEqual(1, check.http.open.call_args.kwargs["timeout"])

    def test_static_fixture_supplies_required_instance_values_and_test_capacity(self):
        fixture = Path(__file__).resolve().parents[1] / "fixtures" / "capacity-gpus.json"
        gpus = json.loads(fixture.read_text())
        self.assertEqual(4, sum(gpu["capacity"] for gpu in gpus))
        for gpu in gpus:
            for instance in gpu["instanceTypes"]:
                self.assertTrue(instance["name"])
                self.assertEqual("NCP.GPU." + gpu["name"], instance["value"])
                self.assertEqual(instance["value"] + "_1x", instance["name"])

    def test_startup_restarts_agent_only_when_effective_configuration_is_stale(self):
        for stale in (False, True):
            with self.subTest(stale=stale):
                check = RecoveryCheck.__new__(RecoveryCheck)
                check.args = SimpleNamespace(backend="target", discovery_mode="static", expected_capacity=4,
                                             agent_version="candidate", image="sample")
                check.identities = []
                check.save = Mock()
                check.usage = Mock(side_effect=[{"capacity": 4, "allocated": 4, "available": 0},
                                               {"capacity": 4, "allocated": 0, "available": 4}])
                def objects(kind, _namespace=None):
                    if kind == "pods":
                        return [{"metadata": {"name": "nvca-test"}, "status": {
                            "phase": "Running", "containerStatuses": [{"ready": True}]}}]
                    return [{"metadata": {"name": "target"}, "spec": {
                        "clusterConfig": {"gpuDiscovery": {"static": {}}}}}]

                check.objects = objects
                restarted = False

                def kubectl(*args):
                    nonlocal restarted
                    if args[0] == "get":
                        return json.dumps({"data": {"config.yaml": "agent:\n  staticGPUCapacity: 4\n"}})
                    if args[:2] == ("rollout", "restart"):
                        restarted = True
                    if args[0] == "logs":
                        mode = "true" if stale and not restarted else "false"
                        return ("version candidate with options: HeartbeatInterval:300000000000, "
                                + "DynamicGPUDiscoveryEnabled:" + mode + ",")
                    return ""

                check.kubectl = Mock(side_effect=kubectl)

                def wait(label, condition, _timeout):
                    if label == "all test capacity available before fixture creation":
                        self.assertFalse(condition(), "previous workers must finish releasing their capacity")
                    self.assertTrue(condition(), label)

                check.wait = wait
                check.cli = Mock(side_effect=RuntimeError("stop before fixtures"))
                with self.assertRaisesRegex(RuntimeError, "stop before fixtures"):
                    check.run()
                self.assertEqual(stale, restarted)
                self.assertEqual(2, check.usage.call_count, "capacity must be ready before creating a function")

    def test_prepared_discovery_requires_backend_mode_and_updated_agent_config(self):
        def backend(mode, name="target"):
            return {"metadata": {"name": name}, "spec": {"clusterConfig": {"gpuDiscovery": {mode: {}}}}}
        check = lambda b, c, mode: prepared_discovery(b, c, "target", mode, 4)
        self.assertTrue(check([backend("dynamic")], "agent: {}", "dynamic"))
        self.assertTrue(check([backend("static")], "agent:\n  staticGPUCapacity: 4\n", "static"))
        self.assertFalse(check([backend("dynamic")], "  staticGPUCapacity: 4\n", "static"))
        self.assertFalse(check([backend("static")], "agent: {}", "static"))
        self.assertFalse(check([backend("dynamic")], "  staticGPUCapacity: 4\n", "dynamic"))
        self.assertFalse(check([backend("static", "different")], "  staticGPUCapacity: 4\n", "static"))

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

    def test_heartbeat_timestamp_belongs_to_the_latest_complete_request(self):
        logs = ('2026-01-01T00:00:00Z INFO heartBeatRequest {"nvcaAgentVersion":"old"}\n'
                '2026-01-01T00:05:00Z INFO heartBeatRequest {"nvcaAgentVersion":"candidate"}\n'
                '2026-01-01T00:06:00Z INFO heartBeatRequest incomplete')
        heartbeat, received = latest_heartbeat(logs, with_timestamp=True)
        self.assertEqual({"nvcaAgentVersion": "candidate"}, heartbeat)
        self.assertEqual(1767225900, received)

    def test_structured_heartbeat_log_preserves_request_and_timestamp(self):
        heartbeat = {"nvcaAgentVersion": "candidate", "gpuUsage": {"H100": {"allocated": 4}}}
        line = json.dumps({"@timestamp": "2026-01-01T00:05:00.123456789Z",
                           "message": "HEART_BEAT_LOGGING: heartBeatRequest " + json.dumps(heartbeat)})
        parsed, received = latest_heartbeat(line, with_timestamp=True)
        self.assertEqual(heartbeat, parsed)
        self.assertAlmostEqual(1767225900.123456, received, places=5)

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
