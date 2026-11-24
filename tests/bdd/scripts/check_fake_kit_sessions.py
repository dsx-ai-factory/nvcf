#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise session exits through real NVCF, NVCA, utils probes, and a kubelet."""

import argparse
import base64
import datetime
import hashlib
import json
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zlib

from check_capacity_recovery import (
    allocation_released, capacity_publication_observed, latest_heartbeat,
    occupied_by_candidate, periodic_heartbeat_seconds,
)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def timestamp(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def container_args(source):
    encoded = base64.b64encode(zlib.compress(source)).decode()
    program = 'import base64,zlib;exec(zlib.decompress(base64.b64decode("' + encoded + '")))'
    return shlex.join(["python3", "-u", "-c", program])


def session_reply(value):
    if isinstance(value, str):
        try:
            return session_reply(json.loads(value))
        except ValueError:
            return None
    if isinstance(value, dict):
        if "session" in value and value.get("state") in ("running", "ended", "idle"):
            return value
        for child in value.values():
            found = session_reply(child)
            if found:
                return found
    return None


def pod_view(pod):
    if pod is None:
        return None
    spec, status = pod["spec"], pod.get("status", {})
    return {"name": pod["metadata"]["name"], "uid": pod["metadata"]["uid"],
            "created": pod["metadata"]["creationTimestamp"],
            "deleting": pod["metadata"].get("deletionTimestamp"),
            "startTime": status.get("startTime"), "conditions": status.get("conditions", []),
            "containers": [{"name": c["name"], "image": c["image"],
                            "argsSHA256": hashlib.sha256(json.dumps(c.get("args", [])).encode()).hexdigest(),
                            "readinessProbe": c.get("readinessProbe")} for c in spec["containers"]],
            "containerStatuses": status.get("containerStatuses", [])}


def ready(pod):
    return bool(pod and not pod.get("deleting") and any(
        c["type"] == "Ready" and c["status"] == "True" for c in pod["conditions"]))


def inference(pod):
    return next((c for c in (pod or {}).get("containerStatuses", []) if c["name"] == "inference"), {})


def watched_statuses(text):
    for line in text.splitlines():
        # kubectl's watch printer can expand requested tabs into padded columns.
        parts = line.split(None, 3)
        if len(parts) == 4:
            try:
                yield parts[0], parts[1], parts[2], json.loads(parts[3])
            except ValueError:
                continue


class SessionCheck:
    def __init__(self, args):
        self.args = args
        self.output = Path(tempfile.mkdtemp(prefix="fake-kit-", dir=args.output_dir))
        self.run_id = self.output.name
        self.config = self.output / (self.run_id + ".yaml")
        shutil.copyfile(args.cli_config_template, self.config)
        self.config.chmod(0o600)
        self.state = Path.home() / (".nvcf-cli." + self.config.stem + ".state")
        if self.state.exists():
            raise RuntimeError("unique CLI credential state unexpectedly exists")
        self.token = ""
        self.identities = []
        self.target = None
        self.phase = "setup"
        self.started = utc()
        self.last_pods = []
        self.last_requests = []
        self.last_signature = None
        self.sequence = 0
        self.original_dto = None
        self.watch = None
        self.watch_stream = None
        self.saw_utils_readiness = False
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def save(self, name, value):
        path = self.output / name
        path.write_text(json.dumps(value, indent=2) + "\n")
        path.chmod(0o600)

    def progress(self, message):
        line = utc() + " " + self.phase + ": " + message
        path = self.output / "progress.log"
        with path.open("a") as stream:
            stream.write(line + "\n")
        path.chmod(0o600)
        print(line, flush=True)

    def command(self, argv, timeout=600):
        if getattr(self, "deadline", None) is not None:
            timeout = min(timeout, self.deadline - time.monotonic())
            if timeout <= 0:
                raise TimeoutError("observation deadline elapsed")
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        if result.returncode:
            code = re.search(r"API error (\d{3})", result.stderr + result.stdout)
            suffix = " (HTTP " + code[1] + ")" if code else ""
            raise RuntimeError(Path(argv[0]).name + " command failed" + suffix)
        return result.stdout.strip()

    def cli(self, *args, timeout=600):
        return self.command([self.args.cli, "--config", str(self.config), *args], timeout)

    def kube(self, *args):
        return self.command(["kubectl", "--context", self.args.context, *args])

    def objects(self, kind, namespace=None):
        scope = ["-n", namespace] if namespace else ["-A"]
        return json.loads(self.kube("get", kind, *scope, "-o", "json"))["items"]

    def api(self, path, host):
        request = urllib.request.Request(self.args.api_url + path,
                                        headers={"Host": host, "Authorization": "Bearer " + self.token})
        timeout = 15
        if getattr(self, "deadline", None) is not None:
            timeout = min(timeout, self.deadline - time.monotonic())
            if timeout <= 0:
                raise TimeoutError("observation deadline elapsed")
        with self.http.open(request, timeout=timeout) as response:
            return json.load(response)

    def cluster(self):
        result = self.api("/v1/accounts/" + self.args.account + "/clusters", self.args.icms_host)
        clusters = result["clusters"] if isinstance(result, dict) else result
        return next(c for c in clusters if c.get("clusterName") == self.args.backend)

    def snapshot(self):
        self.last_pods = self.objects("pods", "nvcf-backend")
        self.last_requests = self.objects("icmsrequests")
        targets = [p for p in self.last_pods if self.target and
                   p["metadata"].get("labels", {}).get("function-id") == self.target[0]]
        pods = [pod_view(p) for p in targets]
        pod = next((p for p in pods if not p["deleting"]), pods[0] if pods else None)
        cluster = self.cluster()
        status = None
        if self.target:
            status = self.api("/v2/nvcf/functions/" + self.target[0] + "/versions/" + self.target[1],
                              self.args.api_host)["function"]["status"]
        sample = {"time": utc(), "phase": self.phase, "pods": pods, "target": pod,
                  "functionStatus": status, "usage": cluster.get("gpuUsage", {}).get(self.args.gpu, {}),
                  "clusterStatus": cluster.get("status")}
        sample["instances"] = [
            {"request": request["metadata"]["name"], "id": name,
             **{key: instance.get(key) for key in ("status", "lastReportedStatus", "lastReportedTimestamp")}}
            for request in self.last_requests
            if self.target and (request.get("spec", {}).get("functionDetails", {}).get("functionId")
                               or request.get("spec", {}).get("functionId")) == self.target[0]
            for name, instance in request.get("status", {}).get("instances", {}).items()]
        for p in pods:
            statuses = {c["name"]: c for c in p["containerStatuses"]}
            if statuses.get("inference", {}).get("ready") and not statuses.get("utils", {}).get("ready"):
                self.saw_utils_readiness = True
        journal = self.output / "timeline.jsonl"
        with journal.open("a") as stream:
            stream.write(json.dumps(sample) + "\n")
        journal.chmod(0o600)
        signature = json.dumps(pods, sort_keys=True)
        if signature != self.last_signature:
            self.sequence += 1
            self.save(f"pod-{self.sequence:04d}.json", sample)
            self.last_signature = signature
        return sample

    def wait(self, label, condition, timeout, *, deadline=None):
        previous = getattr(self, "deadline", None)
        limits = [time.monotonic() + timeout]
        limits += [d for d in (previous, deadline) if d is not None]
        self.deadline, next_progress = min(limits), 0
        try:
            while time.monotonic() < self.deadline:
                try:
                    sample = self.snapshot()
                    result = condition(sample)
                except (TimeoutError, subprocess.TimeoutExpired):
                    if time.monotonic() >= self.deadline:
                        break
                    raise
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    break
                if result:
                    return result
                if time.monotonic() > next_progress:
                    p = sample["target"]
                    self.progress(label + "; function=" + str(sample["functionStatus"]) +
                                  ", restarts=" + str(inference(p).get("restartCount")) +
                                  ", ready=" + str(ready(p)))
                    next_progress = time.monotonic() + 45
                time.sleep(min(1, remaining))
        finally:
            self.deadline = previous
        raise RuntimeError(label + " timed out")

    def set_version(self, version):
        cm = json.loads(self.kube("get", "configmap/nvcfbackend-self-managed", "-n", "nvca-operator", "-o", "json"))
        dto = cm["data"]["cluster-dto.yaml"]
        if self.original_dto is None:
            current = re.search(r'(?m)^\s*nvcaVersion:\s*["\']?([^\s"\']+)', dto)
            if not current or current[1] != self.args.green_agent_version:
                raise RuntimeError("green version must match the original installed agent")
            self.original_dto = dto
            self.save("original-agent-config.json", {"data": {"cluster-dto.yaml": dto}})
        changed, count = re.subn(r'(?m)^(\s*nvcaVersion:\s*).+$',
                                 lambda m: m[1] + json.dumps(version), dto)
        if count != 1:
            raise RuntimeError("expected one self-managed agent version setting")
        self.patch_dto(changed)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            pods = [p for p in self.objects("pods", "nvca-system")
                    if p["metadata"]["name"].startswith("nvca-")
                    and p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
            if len(pods) == 1 and not pods[0]["metadata"].get("deletionTimestamp") and \
                    pods[0].get("status", {}).get("containerStatuses") and all(
                    c.get("ready") for c in pods[0].get("status", {}).get("containerStatuses", [])):
                logs = self.kube("logs", pods[0]["metadata"]["name"], "-c", "agent", "-n", "nvca-system", "--tail=10000")
                if "version " + version + " with options" in logs and periodic_heartbeat_seconds(logs) == 300:
                    self.save("agent-" + self.phase + ".json", {
                        "version": version, "containers": [{"name": c["name"], "image": c["image"],
                         "imageID": c.get("imageID")} for c in pods[0]["status"]["containerStatuses"]],
                        "periodicHeartbeatSeconds": 300})
                    self.progress("running verified agent " + version)
                    return
            time.sleep(1)
        raise RuntimeError("agent version rollout timed out")

    def patch_dto(self, dto):
        path = self.output / "agent-version-patch.json"
        self.save(path.name, {"data": {"cluster-dto.yaml": dto}})
        self.kube("patch", "configmap/nvcfbackend-self-managed", "-n", "nvca-operator", "--type=merge",
                  "--patch-file", str(path))

    def create(self, name, image, args=None):
        command = ["function", "create", "--name", self.run_id + "-" + name, "--image", image,
                   "--inference-url", "/session" if args else "/echo", "--inference-port", "8000",
                   "--health-uri", "/health", "--health-port", "8000", "--health-timeout", "PT30S"]
        if args:
            command += ["--container-args", args]
        created = json.loads(self.cli(*command, "--json"))["function"]
        identity = (created["id"], created["versionId"])
        self.identities.append(identity)
        self.save("functions.json", self.identities)
        self.cli("function", "deploy", "create", "--function-id", identity[0], "--version-id", identity[1],
                 "--backend", self.args.backend, "--gpu", self.args.gpu, "--instance-type", self.args.instance_type,
                 "--regions", self.args.region, "--min-instances", "1", "--max-instances", "1", "--timeout", "600")
        self.progress(name + " deployed")
        return identity

    def start_status_watch(self):
        # Select only status and identity in kubectl itself; never record Pod envs.
        if self.watch is not None:
            self.watch.terminate()
            self.watch.wait(timeout=5)
            self.watch_stream.close()
        self.watch_stream = (self.output / "container-status-watch.tsv").open("a")
        (self.output / "container-status-watch.tsv").chmod(0o600)
        expression = ('{.type}{"\\t"}{.object.metadata.name}{"\\t"}{.object.metadata.uid}'
                      '{"\\t"}{.object.status}{"\\n"}')
        self.watch = subprocess.Popen(["kubectl", "--context", self.args.context, "get", "pods", "-n", "nvcf-backend",
            "-l", "function-id=" + self.target[0], "--watch", "--output-watch-events", "-o", "jsonpath=" + expression],
            stdout=self.watch_stream, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)

    def invoke(self, action, session=None, exit_code=0):
        body = {"action": action, "session": session, "exitCode": exit_code}
        output = self.cli("function", "invoke", "--function-id", self.target[0], "--version-id", self.target[1],
                          "--request-body", json.dumps(body), "--timeout", "120", "--json")
        offset = output.find("{")
        response = session_reply(json.loads(output[offset:])) if offset >= 0 else None
        expected = {"resume": "running", "end": "ended", "status": "idle"}[action]
        if not response or response["state"] != expected or response["session"] != session:
            raise RuntimeError("invocation did not return the fake Kit session response")
        return response

    @staticmethod
    def same_ready(sample, uid):
        p = sample["target"]
        return bool(p and p["uid"] == uid and ready(p) and sample["functionStatus"] == "ACTIVE")

    def age_pod(self, uid):
        def old_enough(sample):
            p = sample["target"]
            if not p or p["uid"] != uid:
                raise RuntimeError("worker changed before session testing")
            start = max(timestamp(p["created"]), timestamp(p["startTime"]))
            return self.same_ready(sample, uid) and time.time() - start > self.args.minimum_pod_age + 2
        self.wait("waiting for actual pod age above the production threshold", old_enough,
                  self.args.minimum_pod_age + 300)

    def clean_ends(self, uid, expect_purge):
        self.start_status_watch()
        for number in range(1, self.args.session_ends + 1):
            before = self.wait("ready before End", lambda s: s if self.same_ready(s, uid) else None, 240)
            p = before["target"]
            self.save(self.phase + f"-before-end-{number}.json", before)
            session = self.run_id + "-" + self.phase + "-" + str(number)
            self.invoke("resume", session)
            self.invoke("end", session)
            self.progress("successful End " + str(number) + " returned; waiting for restart")
            restarts = inference(p).get("restartCount", 0)
            ended = time.monotonic()
            utils_became_unready = False

            def settled(sample):
                nonlocal utils_became_unready
                pod = sample["target"]
                if not pod or pod["uid"] != uid or pod.get("deleting"):
                    if not expect_purge:
                        raise RuntimeError("fixed agent purged a successful-session worker")
                    return {"purged": True, "ends": number, "before": before, "after": sample}
                status = inference(pod)
                containers = {c["name"]: c for c in pod["containerStatuses"]}
                if status.get("ready") and not containers.get("utils", {}).get("ready"):
                    utils_became_unready = True
                last = status.get("lastState", {}).get("terminated") or status.get("state", {}).get("terminated")
                if last and last.get("exitCode") != 0:
                    raise RuntimeError("clean End did not exit with code zero")
                if status.get("restartCount", 0) > restarts and utils_became_unready and self.same_ready(sample, uid):
                    if not last or last.get("exitCode") != 0:
                        raise RuntimeError("missing successful termination evidence after restart")
                    return {"purged": False, "ends": number, "after": sample,
                            "utilsDrivenNotReadyObserved": True,
                            "restartReadySeconds": time.monotonic() - ended}
                return None

            result = self.wait("session restart or expected red-control purge", settled, 300)
            self.save(self.phase + f"-after-end-{number}.json", result)
            self.collect_logs(self.phase + "-end-" + str(number))
            if result["purged"]:
                return result
        if expect_purge:
            raise RuntimeError("red control did not reproduce the cumulative-restart purge")
        return result

    def recover(self, green_uid, old_name, periodic_deadline, publication_since):
        self.wait("nonzero exit worker purged and allocation released", lambda _s: allocation_released(
            old_name, self.last_pods, self.last_requests), 360, deadline=periodic_deadline)
        released = time.monotonic()

        def published_by_capacity_check(sample):
            logs = self.kube("logs", "deployment/nvca", "-c", "agent", "-n", "nvca-system",
                             "--since-time=" + publication_since, "--tail=10000")
            return capacity_publication_observed(logs)

        self.wait("released capacity published", published_by_capacity_check, 60,
                  deadline=periodic_deadline)
        published = time.monotonic() - released
        final = self.wait("automatic healthy replacement after nonzero exit", lambda s: s if s["target"] and
            s["target"]["uid"] != green_uid and ready(s["target"]) else None,
            max(1, 180 - (time.monotonic() - released)), deadline=periodic_deadline)
        replacement_ready = time.monotonic() - released
        replacement_uid = final["target"]["uid"]
        self.save("crash-readiness.json", {"publicationSeconds": published,
            "replacementReadySeconds": replacement_ready, "sample": final})
        self.progress("replacement Pod Ready; waiting separately for function ACTIVE")

        def active(sample):
            pod = sample["target"]
            if not pod or pod["uid"] != replacement_uid or pod.get("deleting"):
                raise RuntimeError("replacement worker changed while waiting for function ACTIVE")
            return self.same_ready(sample, replacement_uid)

        # The function API reconciles independently of kubelet readiness.
        self.wait("replacement function ACTIVE", active, self.args.function_active_timeout)
        function_active = time.monotonic() - released
        self.invoke("status")
        return {"crashPublicationSeconds": published, "crashReplacementReadySeconds": replacement_ready,
                "publicationTrigger": "capacity",
                "crashFunctionActiveSeconds": function_active,
                "crashReadyToActiveSeconds": function_active - replacement_ready,
                "replacementInvocationPassed": True, "crashReplacementUID": replacement_uid}

    def collect_logs(self, label):
        ids = [self.target[0]] if self.target else []
        for namespace, deployment in (("nvca-system", "nvca"), ("sis", "spot-instance-service"), ("nvcf", "nvcf-api")):
            try:
                logs = self.kube("logs", "deployment/" + deployment, "-n", namespace,
                                 "--since-time=" + self.started, "--tail=100000")
                lines = [line for line in logs.splitlines() if " with options:" not in line and
                         (any(i in line for i in ids) or any(term in line for term in (
                             "heartBeatRequest", "cloud-health", "cloudHealth", "heartbeat_trigger", "no available")))]
                text = "\n".join(lines)
                for secret in (self.token, json.loads(self.state.read_text()).get("apiKey", "")):
                    if secret:
                        text = text.replace(secret, "[REDACTED]")
                text = re.sub(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[REDACTED]", text)
                path = self.output / (label + "-" + deployment + ".log")
                path.write_text(text + "\n")
                path.chmod(0o600)
            except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired):
                pass
        for p in self.last_pods:
            if self.target and p["metadata"].get("labels", {}).get("function-id") == self.target[0]:
                try:
                    text = self.kube("logs", p["metadata"]["name"], "-n", "nvcf-backend", "-c", "inference", "--previous")
                    path = self.output / (label + "-inference-previous.log")
                    path.write_text(text + "\n")
                    path.chmod(0o600)
                except (RuntimeError, OSError, subprocess.TimeoutExpired):
                    pass

    def run(self):
        try:
            self.cli("init")
            self.token = json.loads(self.state.read_text())["token"]
            self.save("setup.json", {"context": self.args.context, "backend": self.args.backend,
                "redVersion": self.args.red_agent_version, "greenVersion": self.args.green_agent_version,
                "minimumPodAgeSeconds": self.args.minimum_pod_age, "observationSeconds": self.args.observation_seconds,
                "sessionEnds": self.args.session_ends, "fakeKitSHA256": hashlib.sha256(Path(self.args.fixture).read_bytes()).hexdigest()})
            self.wait("no pre-existing worker workloads", lambda _s: not self.last_pods, 360)
            self.phase = "red"
            self.set_version(self.args.red_agent_version)
            self.wait("all capacity available", lambda s: s["usage"].get("available") == self.args.capacity, 360)
            for index in range(self.args.capacity - 1):
                self.create("filler-" + str(index), self.args.filler_image)
            self.target = self.create("sessions", self.args.fake_kit_image, container_args(Path(self.args.fixture).read_bytes()))
            self.cli("api-key", "generate", "--for", "function", "--description", self.run_id, "--expires-in", "2h",
                     "--scopes", "invoke_function,list_functions,queue_details,list_functions_details")
            initial = self.wait("all slots occupied and target Ready", lambda s: s if ready(s["target"]) and
                                s["usage"].get("allocated") == self.args.capacity and s["usage"].get("available") == 0 else None, 360)
            specs = {c["name"]: c for c in initial["target"]["containers"]}
            if specs["inference"]["readinessProbe"] or not specs["utils"]["readinessProbe"].get("httpGet"):
                raise RuntimeError("test requires inference without a probe and real utils HTTP readiness")
            if inference(initial["target"]).get("restartCount") != 0:
                raise RuntimeError("initial fake Kit worker already restarted before testing")
            red_uid = initial["target"]["uid"]
            self.age_pod(red_uid)
            red = self.clean_ends(red_uid, expect_purge=True)
            self.save("red-result.json", red)
            replacement = self.wait("automatic replacement after red purge", lambda s: s if s["target"] and
                s["target"]["uid"] != red_uid and ready(s["target"]) and s["functionStatus"] == "ACTIVE" else None, 600)
            self.collect_logs("red-complete")
            self.phase = "green"
            self.set_version(self.args.green_agent_version)
            green_uid = replacement["target"]["uid"]
            self.age_pod(green_uid)
            green = self.clean_ends(green_uid, expect_purge=False)
            self.save("green-clean-ends.json", green)
            observation_start = time.monotonic()
            observation_restarts = inference(green["after"]["target"])["restartCount"]
            transient_unhealthy = 0

            def observed(sample):
                nonlocal transient_unhealthy
                if not sample["target"] or sample["target"]["uid"] != green_uid or sample["target"].get("deleting"):
                    raise RuntimeError("worker was purged during unattended observation")
                if inference(sample["target"]).get("restartCount") != observation_restarts:
                    raise RuntimeError("inference restarted without a session End during observation")
                if not self.same_ready(sample, green_uid):
                    transient_unhealthy += 1
                return (time.monotonic() - observation_start >= self.args.observation_seconds
                        and self.same_ready(sample, green_uid))

            self.wait("unattended observation with deployment retained", observed, self.args.observation_seconds + 30)
            if not self.saw_utils_readiness:
                raise RuntimeError("never observed inference running while utils drove Pod readiness false")
            self.save("green-observation.json", {"passed": True, "uid": green_uid,
                       "seconds": time.monotonic() - observation_start, "utilsDrivenReadinessObserved": True,
                       "transientUnhealthySamples": transient_unhealthy})
            self.collect_logs("green-observation")
            self.phase = "crash-recovery"
            self.start_status_watch()
            since = utc()

            def occupied(sample):
                if not self.same_ready(sample, green_uid):
                    raise RuntimeError("worker changed while establishing the crash-recovery heartbeat phase")
                logs = self.kube("logs", "deployment/spot-instance-service", "-n", "sis", "--since-time=" + since,
                                 "--tail=10000")
                observed = latest_heartbeat(logs, with_timestamp=True)
                heartbeat, received = observed if observed else (None, None)
                if occupied_by_candidate(sample["usage"], heartbeat, self.args.gpu,
                                         self.args.capacity, self.args.green_agent_version):
                    return {"heartbeat": heartbeat, "heartbeatReceivedAt": received, "observedAt": utc()}

            occupied_snapshot = self.wait("fresh occupied periodic heartbeat before nonzero exit", occupied, 360)
            self.save("crash-occupied.json", occupied_snapshot)
            periodic_deadline = time.monotonic() + occupied_snapshot["heartbeatReceivedAt"] + 300 - time.time()
            before = self.snapshot()
            old_name = before["target"]["name"]
            session = self.run_id + "-crash"
            self.invoke("resume", session)
            self.invoke("end", session, exit_code=1)
            recovery = self.recover(green_uid, old_name, periodic_deadline, occupied_snapshot["observedAt"])
            exits = [run.get("exitCode") for _event, _name, uid, status in watched_statuses(
                (self.output / "container-status-watch.tsv").read_text()) if uid == green_uid
                for c in status.get("containerStatuses", []) if c["name"] == "inference"
                for run in (c.get("state", {}).get("terminated"), c.get("lastState", {}).get("terminated")) if run]
            if 1 not in exits:
                raise RuntimeError("missing kubelet exit-code-one evidence for crash recovery")
            self.collect_logs("crash-recovery")
            result = {"passed": True, "redPurgedCleanExit": True, "redEnds": red["ends"],
                      "greenCleanEnds": self.args.session_ends, "greenUIDPreserved": green_uid,
                      "observationSeconds": self.args.observation_seconds, "utilsDrivenReadinessObserved": True,
                      **recovery, "physicalKitTested": False}
            self.save("result.json", result)
            print("fake Kit sessions passed: " + json.dumps(result), flush=True)
        finally:
            failed = sys.exc_info()[0] is not None
            errors = []
            if failed:
                self.collect_logs("failure")
            if self.watch is not None:
                self.watch.terminate()
                try:
                    self.watch.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.watch.kill()
                    self.watch.wait(timeout=5)
                self.watch_stream.close()
            for fid, vid in reversed(self.identities):
                try:
                    self.cli("function", "delete", "--function-id", fid, "--version-id", vid, timeout=90)
                except (RuntimeError, subprocess.TimeoutExpired):
                    errors.append("function cleanup")
            if self.state.exists() and self.token:
                try:
                    keys = json.loads(self.cli("api-key", "list", "--json"))["keys"]
                    for key in keys:
                        if key.get("description") == self.run_id:
                            self.cli("api-key", "revoke", key["id"], "--force", timeout=60)
                except (RuntimeError, subprocess.TimeoutExpired):
                    errors.append("invocation key cleanup")
            if self.original_dto is not None:
                try:
                    self.patch_dto(self.original_dto)
                    self.phase = "restore"
                    self.set_version(self.args.green_agent_version)
                except (RuntimeError, subprocess.TimeoutExpired):
                    errors.append("agent restoration")
            self.state.unlink(missing_ok=True)
            self.save("cleanup.json", {"errors": errors})
            if errors and not failed:
                raise RuntimeError("; ".join(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("cli", "cli-config-template", "context", "backend", "red-agent-version", "green-agent-version",
                 "fake-kit-image", "filler-image", "fixture", "gpu", "instance-type", "region", "account",
                 "api-url", "api-host", "icms-host", "output-dir"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--capacity", type=int, default=4)
    parser.add_argument("--minimum-pod-age", type=int, default=600)
    parser.add_argument("--observation-seconds", type=int, default=600)
    parser.add_argument("--session-ends", type=int, default=4)
    parser.add_argument("--function-active-timeout", type=int, default=90)
    args = parser.parse_args()
    if not args.context.startswith("k3d-") or args.capacity < 1 or args.minimum_pod_age < 600 or \
            args.observation_seconds < 600 or args.session_ends < 3 or args.function_active_timeout <= 0:
        parser.error("requires local k3d, positive capacity, production age threshold, and ten-minute observation")
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    def interrupted(_signal, _frame):
        raise RuntimeError("session test interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        SessionCheck(args).run()
    except (RuntimeError, OSError, ValueError, KeyError, urllib.error.URLError, subprocess.TimeoutExpired) as error:
        print("fake Kit sessions failed: " + (str(error) if isinstance(error, RuntimeError) else type(error).__name__), flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
