# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import json
import os
from pathlib import Path
import select
import shlex
import subprocess
import sys
import unittest
from unittest import mock
import urllib.request
from types import SimpleNamespace

from check_fake_kit_sessions import SessionCheck, container_args, pod_view, ready, session_reply, watched_statuses


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "fake_kit.py"
SPEC = importlib.util.spec_from_file_location("fake_kit_fixture", FIXTURE)
FAKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FAKE)


class FakeKitTests(unittest.TestCase):
    def test_recovery_bounds_pod_readiness_and_function_status_separately(self):
        check = SessionCheck.__new__(SessionCheck)
        check.args = SimpleNamespace(function_active_timeout=90)
        check.save, check.progress, check.invoke = mock.Mock(), mock.Mock(), mock.Mock()
        check.kube = mock.Mock(return_value='msg="Published NVCA heartbeat" heartbeat_trigger=capacity')
        clock = [0]
        pod = {"uid": "replacement", "conditions": [{"type": "Ready", "status": "True"}]}
        sample = {"target": pod, "functionStatus": "DEGRADING"}

        def wait(label, condition, timeout, **kwargs):
            if label == "nonzero exit worker purged and allocation released":
                return True
            if label == "released capacity published":
                self.assertEqual(300, kwargs["deadline"])
                self.assertEqual(60, timeout)
                self.assertTrue(condition({"usage": {"available": 0}}),
                                "replacement may consume the published capacity before this poll")
                self.assertIn("--since-time=cutoff", check.kube.call_args.args)
                check.kube.return_value = 'msg="Published NVCA heartbeat" heartbeat_trigger=periodic'
                self.assertFalse(condition({"usage": {"available": 0}}))
                clock[0] = 14
                return True
            if label == "automatic healthy replacement after nonzero exit":
                self.assertEqual(300, kwargs["deadline"])
                self.assertEqual(166, timeout)
                self.assertEqual(sample, condition(sample), "Pod readiness must not wait for the function API")
                clock[0] = 125
                return sample
            self.assertEqual("replacement function ACTIVE", label)
            self.assertEqual(90, timeout)
            self.assertFalse(condition(sample))
            self.assertTrue(condition({**sample, "functionStatus": "ACTIVE"}))
            clock[0] = 185
            return True

        check.wait = wait
        with mock.patch("check_fake_kit_sessions.time.monotonic", side_effect=lambda: clock[0]):
            result = check.recover("original", "old-name", 300, "cutoff")
        self.assertEqual(125, result["crashReplacementReadySeconds"])
        self.assertEqual(185, result["crashFunctionActiveSeconds"])
        self.assertEqual(60, result["crashReadyToActiveSeconds"])
        check.invoke.assert_called_once_with("status")

    def test_created_identity_does_not_depend_on_a_subsequent_cli_state_read(self):
        check = SessionCheck.__new__(SessionCheck)
        check.args = SimpleNamespace(backend="local", gpu="H100", instance_type="one", region="local")
        check.identities, check.run_id = [], "unit"
        check.save, check.progress = mock.Mock(), mock.Mock()
        check.cli = mock.Mock(side_effect=[json.dumps({"function": {"id": "owned", "versionId": "version"}}),
                                           RuntimeError("deployment failed")])
        with self.assertRaisesRegex(RuntimeError, "deployment failed"):
            check.create("fixture", "runtime")
        self.assertEqual([("owned", "version")], check.identities)
        self.assertFalse(any(call.args[0] == "status" for call in check.cli.call_args_list))

    def test_wait_rejects_a_late_snapshot_and_restores_the_previous_budget(self):
        check = SessionCheck.__new__(SessionCheck)
        check.deadline = 5
        clock = [0]
        check.snapshot = mock.Mock(return_value={})
        check.progress = mock.Mock()

        def observed(_sample):
            clock[0] = 6
            return True

        with mock.patch("check_fake_kit_sessions.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(RuntimeError, "bounded timed out"):
                check.wait("bounded", observed, 60)
        self.assertEqual(5, check.deadline)

    def test_default_warmup_spans_two_real_utils_probe_periods(self):
        with mock.patch.object(FAKE.time, "monotonic", return_value=10):
            self.assertEqual(40, FAKE.Sessions().ready_at)

    def test_next_session_waits_for_utils_readiness_to_fall_and_recover(self):
        def sample(restarts, utils_ready):
            return {"functionStatus": "ACTIVE", "target": {
                "uid": "uid", "conditions": [{"type": "Ready", "status": "True" if utils_ready else "False"}],
                "containerStatuses": [{"name": "inference", "ready": True, "restartCount": restarts,
                    "lastState": {"terminated": {"exitCode": 0}}}, {"name": "utils", "ready": utils_ready}]}}

        check = SessionCheck.__new__(SessionCheck)
        check.args = SimpleNamespace(session_ends=1)
        check.phase, check.run_id = "green", "unit"
        for name in ("save", "progress", "collect_logs", "start_status_watch", "invoke"):
            setattr(check, name, mock.Mock())

        def wait(label, condition, _timeout):
            if label == "ready before End":
                return condition(sample(0, True))
            self.assertIsNone(condition(sample(1, True)), "stale utils Ready must not finish the cycle")
            self.assertIsNone(condition(sample(1, False)), "the new process is still warming up")
            return condition(sample(1, True))

        check.wait = wait
        result = check.clean_ends("uid", expect_purge=False)
        self.assertTrue(result["utilsDrivenNotReadyObserved"])
        self.assertFalse(result["purged"])
        self.assertEqual(2, check.invoke.call_count)

    def test_only_an_active_matching_session_can_end(self):
        sessions = FAKE.Sessions(0)
        with self.assertRaises(ValueError):
            sessions.apply("end", "missing")
        self.assertEqual({"state": "running", "session": "one"}, sessions.apply("resume", "one"))
        with self.assertRaises(ValueError):
            sessions.apply("resume", "two")
        with self.assertRaises(ValueError):
            sessions.apply("end", "different")
        with self.assertRaises(ValueError):
            sessions.apply("end", "one", 2)
        self.assertEqual({"state": "ended", "session": "one", "exitCode": 0}, sessions.apply("end", "one"))
        with self.assertRaises(ValueError):
            sessions.apply("end", "one")

    def test_packed_launch_returns_success_before_real_process_exit(self):
        for code in (0, 1):
            with self.subTest(exit_code=code):
                args = shlex.split(container_args(FIXTURE.read_bytes()))
                args[0] = sys.executable
                env = {**os.environ, "PORT": "0", "FAKE_KIT_WARMUP_SECONDS": "0"}
                process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    readable, _, _ = select.select([process.stdout], [], [], 5)
                    self.assertTrue(readable, "fixture failed to boot")
                    boot = json.loads(process.stdout.readline())
                    http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                    for action in ("resume", "end"):
                        data = json.dumps({"action": action, "session": "one", "exitCode": code}).encode()
                        request = urllib.request.Request("http://127.0.0.1:" + str(boot["port"]) + "/session", data=data,
                                                         headers={"Content-Type": "application/json"})
                        with http.open(request, timeout=5) as response:
                            self.assertEqual(200, response.status)
                            body = json.load(response)
                        self.assertEqual("running" if action == "resume" else "ended", body["state"])
                    self.assertEqual(code, process.wait(timeout=5))
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=5)

    def test_session_reply_reads_cli_wrapped_and_encoded_bodies(self):
        body = {"session": "one", "state": "ended", "exitCode": 0}
        self.assertEqual(body, session_reply({"responseBody": json.dumps(body)}))
        self.assertIsNone(session_reply({"requestBody": {"action": "end", "session": "one"}}))

    def test_status_watch_preserves_real_exit_codes_and_ignores_incomplete_lines(self):
        status = {"containerStatuses": [{"name": "inference", "lastState": {"terminated": {"exitCode": 1}}}]}
        text = "incomplete\nMODIFIED\tworker\tuid\t" + json.dumps(status) + "\nDELETED\tworker\tuid\tbad"
        self.assertEqual([("MODIFIED", "worker", "uid", status)], list(watched_statuses(text)))
        padded = "MODIFIED   worker                   uid           " + json.dumps(status)
        self.assertEqual([("MODIFIED", "worker", "uid", status)], list(watched_statuses(padded)))

    def test_saved_pod_view_excludes_environment_and_raw_container_arguments(self):
        pod = {"metadata": {"name": "worker", "uid": "uid", "creationTimestamp": "now"},
               "spec": {"containers": [{"name": "inference", "image": "sample", "args": ["private-value"],
                                         "env": [{"name": "TOKEN", "value": "private-value"}]}]},
               "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        view = pod_view(pod)
        self.assertTrue(ready(view))
        self.assertNotIn("private-value", json.dumps(view))


if __name__ == "__main__":
    unittest.main()
