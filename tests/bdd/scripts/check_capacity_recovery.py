#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise automatic worker replacement on an explicitly selected local stack."""

import argparse
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def ready_workers(pods, function_ids):
    return [
        pod for pod in pods
        if pod["metadata"].get("labels", {}).get("function-id") in function_ids
        and not pod["metadata"].get("deletionTimestamp")
        and any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in pod.get("status", {}).get("conditions", []))
    ]


def allocation_released(old_name, pods, requests):
    if any(p["metadata"]["name"] == old_name for p in pods):
        return False
    instances = [
        instance for req in requests
        for name, instance in req.get("status", {}).get("instances", {}).items()
        if name == old_name or instance.get("id") == old_name
    ]
    return all(i.get("lastReportedStatus") == "terminated" for i in instances)


def latest_heartbeat(logs, *, with_timestamp=False):
    found = None
    for line in logs.splitlines():
        if "heartBeatRequest " not in line:
            continue
        raw = line.split("heartBeatRequest ", 1)[1].replace('\\"', '"')
        try:
            payload = json.JSONDecoder().raw_decode(raw)[0]
            if with_timestamp:
                if line.lstrip().startswith("{"):
                    event = json.loads(line)
                    received_at = event.get("@timestamp") or event.get("timestamp", "")
                else:
                    received_at = line.split()[0]
                received = datetime.datetime.fromisoformat(received_at.replace("Z", "+00:00")).timestamp()
                found = payload, received
            else:
                found = payload
        except (ValueError, json.JSONDecodeError):
            continue
    return found


def occupied_by_candidate(usage, heartbeat, gpu, capacity, version):
    reported = (heartbeat or {}).get("gpuUsage", {}).get(gpu, {})
    return (
        usage.get("capacity") == capacity
        and usage.get("allocated") == capacity
        and usage.get("available") == 0
        and (heartbeat or {}).get("nvcaAgentVersion") == version
        and reported.get("allocated") == capacity
    )


def capacity_publication_observed(logs):
    return any("Published NVCA heartbeat" in line and
               re.search(r'heartbeat_trigger[=:"\s]+capacity\b', line)
               for line in logs.splitlines())


def periodic_heartbeat_seconds(logs):
    intervals = re.findall(r"\bHeartbeatInterval:(\d+)", logs)
    if not intervals:
        raise RuntimeError("agent startup did not report its effective heartbeat interval")
    return int(intervals[-1]) / 1_000_000_000


def discovery_mode(logs):
    modes = re.findall(r"\bDynamicGPUDiscoveryEnabled:(true|false)", logs)
    if not modes:
        raise RuntimeError("agent startup did not report its effective discovery mode")
    return "dynamic" if modes[-1] == "true" else "static"


def prepared_discovery(backends, config, backend, mode, capacity):
    selected = next((b for b in backends if b["metadata"]["name"] == backend), {})
    discovery = selected.get("spec", {}).get("clusterConfig", {}).get("gpuDiscovery", {})
    overrides = re.findall(r"(?m)^\s*staticGPUCapacity:\s*(\d+)\s*$", config)
    configured_capacity = int(overrides[-1]) if overrides else 0
    return mode in discovery and configured_capacity == (capacity if mode == "static" else 0)


class RecoveryCheck:
    def __init__(self, args):
        self.args = args
        self.identities = []
        self.output = Path(tempfile.mkdtemp(prefix="capacity-recovery-", dir=args.output_dir))
        # Credentials remain in memory and never enter argv, stdout or evidence.
        self.token = json.loads(Path(args.state_file).read_text())["token"]
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def save(self, name, value):
        path = self.output / name
        path.write_text(json.dumps(value, indent=2) + "\n")
        path.chmod(0o600)

    def command(self, argv):
        timeout = 600
        if getattr(self, "deadline", None) is not None:
            timeout = min(timeout, self.deadline - time.monotonic())
            if timeout <= 0:
                raise TimeoutError("observation deadline elapsed")
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        if result.returncode:
            # CLI and API failures can contain sensitive data. Keep failures terse.
            raise RuntimeError(f"{Path(argv[0]).name} failed with exit code {result.returncode}")
        return result.stdout.strip()

    def cli(self, *args):
        try:
            return self.command([self.args.cli, "--config", self.args.cli_config, *args])
        except RuntimeError:
            raise RuntimeError("nvcf-cli " + " ".join(args[:2]) + " failed") from None

    def kubectl(self, *args):
        return self.command(["kubectl", "--context", self.args.context, *args])

    def objects(self, kind, namespace=None):
        scope = ["-n", namespace] if namespace else ["-A"]
        return json.loads(self.kubectl("get", kind, *scope, "-o", "json"))["items"]

    def usage(self):
        request = urllib.request.Request(
            self.args.api_url + "/v1/accounts/" + self.args.account + "/clusters",
            headers={"Host": self.args.icms_host, "Authorization": "Bearer " + self.token},
        )
        timeout = 20
        if getattr(self, "deadline", None) is not None:
            timeout = min(timeout, self.deadline - time.monotonic())
            if timeout <= 0:
                raise TimeoutError("observation deadline elapsed")
        with self.http.open(request, timeout=timeout) as response:
            clusters = json.load(response)
        if isinstance(clusters, dict):
            clusters = clusters["clusters"]
        cluster = next(c for c in clusters if c.get("clusterName") == self.args.backend)
        return cluster.get("gpuUsage", {}).get(self.args.gpu, {})

    def wait(self, label, condition, timeout, *, deadline=None):
        previous = getattr(self, "deadline", None)
        limits = [time.monotonic() + timeout]
        limits += [d for d in (previous, deadline) if d is not None]
        self.deadline = min(limits)
        try:
            while time.monotonic() < self.deadline:
                try:
                    value = condition()
                except (RuntimeError, OSError, urllib.error.URLError, subprocess.TimeoutExpired):
                    value = None
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    break
                if value:
                    return value
                time.sleep(min(1, remaining))
        finally:
            self.deadline = previous
        raise RuntimeError(label + " timed out")

    def capacity_published_since(self, since):
        # A replacement can consume capacity before the next availability read.
        logs = self.kubectl("logs", "deployment/nvca", "-c", "agent", "-n", "nvca-system",
                            "--since-time=" + since, "--tail=10000")
        return capacity_publication_observed(logs)

    def create(self, name):
        args = self.args
        created = json.loads(self.cli("function", "create", "--name", name,
            "--image", args.image, "--inference-url", "/echo", "--inference-port", "8000",
            "--health-uri", "/health", "--health-port", "8000", "--health-timeout", "PT30S", "--json"))["function"]
        identity = (created["id"], created["versionId"])
        self.identities.append(identity)
        self.save("functions.json", self.identities)
        self.cli("function", "deploy", "create", "--function-id", identity[0],
                 "--version-id", identity[1], "--backend", args.backend, "--gpu", args.gpu,
                 "--instance-type", args.instance_type, "--regions", args.region,
                 "--min-instances", "1", "--max-instances", "1", "--timeout", "600")

    def run(self):
        args = self.args
        prefix = "bdd-capacity-" + uuid.uuid4().hex[:8]
        try:
            def rollout_complete():
                agents = [p for p in self.objects("pods", "nvca-system")
                          if p["metadata"]["name"].startswith("nvca-")
                          and p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
                return (len(agents) == 1 and not agents[0]["metadata"].get("deletionTimestamp")
                        and bool(agents[0].get("status", {}).get("containerStatuses"))
                        and all(c.get("ready") for c in agents[0]["status"]["containerStatuses"]))

            def configured():
                if not rollout_complete():
                    return False
                startup = self.kubectl("logs", "deployment/nvca", "-n", "nvca-system", "--tail=10000")
                return ("version " + args.agent_version + " with options" in startup
                        and discovery_mode(startup) == args.discovery_mode
                        and periodic_heartbeat_seconds(startup) == 300)

            def prepared():
                config = json.loads(self.kubectl("get", "configmap/agent-config", "-n", "nvca-system", "-o", "json"))
                return prepared_discovery(self.objects("nvcfbackends"), config["data"]["config.yaml"],
                                          args.backend, args.discovery_mode, args.expected_capacity)

            self.wait("backend and agent discovery configuration", prepared, 180)
            try:
                current = configured()
            except (RuntimeError, subprocess.TimeoutExpired):
                current = False
            if not current:
                # The installed operator can update the mounted ConfigMap without
                # restarting an agent that reads its configuration only at startup.
                self.kubectl("rollout", "restart", "deployment/nvca", "-n", "nvca-system")
                self.kubectl("rollout", "status", "deployment/nvca", "-n", "nvca-system", "--timeout=300s")
            self.wait("candidate version and discovery configuration", configured, 180)
            self.save("setup.json", {"agentVersion": args.agent_version,
                                     "discoveryMode": args.discovery_mode})

            def unused():
                usage = self.usage()
                return (usage.get("capacity") == args.expected_capacity
                        and usage.get("allocated") == 0
                        and usage.get("available") == args.expected_capacity)

            # Deleting the previous example's definitions can return before its
            # workers finish terminating and their released capacity is published.
            self.wait("all test capacity available before fixture creation", unused, 360)
            # Independent requests preserve the min=max=1 release behavior.
            for index in range(args.expected_capacity):
                self.create(f"{prefix}-{index}")
            self.save("functions.json", self.identities)
            function_ids = {identity[0] for identity in self.identities}
            workers = lambda: self.objects("pods", "nvcf-backend")
            self.wait("all independent workers ready", lambda: len(ready_workers(
                workers(), function_ids)) == args.expected_capacity, 600)

            # With no free GPUs, a fresh agent cannot send an availability-increase
            # heartbeat. Observe its first occupied periodic heartbeat before loss,
            # giving the test a full default heartbeat interval for its deadline.
            self.kubectl("rollout", "restart", "deployment/nvca", "-n", "nvca-system")
            self.kubectl("rollout", "status", "deployment/nvca", "-n", "nvca-system", "--timeout=300s")

            self.wait("old agent exited", rollout_complete, 300)
            deployment = json.loads(self.kubectl("get", "deployment/nvca", "-n", "nvca-system", "-o", "json"))
            images = [c["image"] for c in deployment["spec"]["template"]["spec"]["containers"]]
            if not any(image.endswith(":" + args.agent_version) for image in images):
                raise RuntimeError("the installed agent image does not match the candidate version")
            startup = self.kubectl("logs", "deployment/nvca", "-n", "nvca-system", "--tail=10000")
            interval = periodic_heartbeat_seconds(startup)
            if interval != 300:
                raise RuntimeError("the test requires the default five-minute heartbeat interval")
            if discovery_mode(startup) != args.discovery_mode:
                raise RuntimeError("the agent is not using the requested GPU discovery mode")
            agent_pods = self.objects("pods", "nvca-system")
            self.save("candidate.json", {"images": images, "heartbeatSeconds": interval,
                "imageIDs": [c.get("imageID") for p in agent_pods
                             for c in p.get("status", {}).get("containerStatuses", [])]})
            # Exclude heartbeats the old process sent while it was terminating.
            since = datetime.datetime.now(datetime.timezone.utc).isoformat()

            def occupied():
                observed = latest_heartbeat(self.kubectl("logs", "deployment/spot-instance-service",
                    "-n", "sis", "--since-time=" + since, "--tail=10000"), with_timestamp=True)
                heartbeat, received = observed if observed else (None, None)
                usage = self.usage()
                if occupied_by_candidate(usage, heartbeat, args.gpu, args.expected_capacity, args.agent_version):
                    return {"usage": usage, "heartbeat": heartbeat, "images": images,
                            "heartbeatReceivedAt": received,
                            "notBefore": since,
                            "observedAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}
                return None

            occupied_snapshot = self.wait("candidate heartbeat with no spare capacity", occupied, 360)
            self.save("occupied.json", occupied_snapshot)
            # A slow release must not move the deadline past the periodic fallback.
            periodic_deadline = time.monotonic() + occupied_snapshot["heartbeatReceivedAt"] + interval - time.time()
            old = next(p for p in ready_workers(workers(), function_ids)
                       if p["metadata"]["labels"]["function-id"] == self.identities[0][0])
            old_name, old_uid = old["metadata"]["name"], old["metadata"]["uid"]
            self.kubectl("delete", "pod", old_name, "-n", "nvcf-backend", "--grace-period=5", "--wait=false")
            self.wait("workload gone and termination reported", lambda: allocation_released(
                old_name, workers(), self.objects("icmsrequests")), 240, deadline=periodic_deadline)
            released = time.monotonic()

            self.wait("released capacity published", lambda: self.capacity_published_since(occupied_snapshot["observedAt"]),
                      args.publication_timeout, deadline=periodic_deadline)
            published = time.monotonic() - released

            def recovered():
                ready = ready_workers(workers(), function_ids)
                return (len(ready) == args.expected_capacity
                        and any(p["metadata"]["labels"]["function-id"] == self.identities[0][0]
                                and p["metadata"]["uid"] != old_uid for p in ready))

            self.wait("automatic replacement ready", recovered,
                      max(1, args.recovery_timeout - (time.monotonic() - released)), deadline=periodic_deadline)
            result = {"passed": True, "agentVersion": args.agent_version, "discoveryMode": args.discovery_mode,
                      "publicationTrigger": "capacity",
                      "capacity": args.expected_capacity, "publicationSeconds": published,
                      "replacementReadySeconds": time.monotonic() - released,
                      "workerGraceSeconds": 5, "periodicHeartbeatSeconds": 300}
            self.save("result.json", result)
            print("capacity recovery passed: " + json.dumps(result))
        finally:
            # Remove this invocation's definitions as well as deployments so repeated
            # runs do not consume the account's function quota.
            failed = sys.exc_info()[0] is not None
            cleanup_errors = 0
            for function_id, version_id in reversed(self.identities):
                try:
                    self.cli("function", "delete", "--function-id", function_id,
                             "--version-id", version_id)
                except (RuntimeError, subprocess.TimeoutExpired):
                    cleanup_errors += 1
            if cleanup_errors:
                self.save("cleanup.json", {"failedFunctionDeletions": cleanup_errors})
                if not failed:
                    raise RuntimeError("test function cleanup failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("cli", "cli-config", "state-file", "context", "backend", "agent-version", "image",
                 "gpu", "instance-type", "region", "account", "api-url", "icms-host", "output-dir"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--expected-capacity", type=int, required=True)
    parser.add_argument("--discovery-mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument("--publication-timeout", type=int, default=60)
    parser.add_argument("--recovery-timeout", type=int, default=180)
    args = parser.parse_args()
    if not args.context.startswith("k3d-"):
        parser.error("this check requires an explicitly selected local k3d context")
    if not 0 < args.publication_timeout <= args.recovery_timeout < 300:
        parser.error("deadlines must be positive and shorter than the default periodic heartbeat")
    if args.expected_capacity < 1:
        parser.error("expected capacity must be positive")
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    try:
        RecoveryCheck(args).run()
    except RuntimeError as error:
        print("capacity recovery failed: " + str(error))
        raise SystemExit(1) from None
    except (OSError, ValueError, KeyError, StopIteration,
            urllib.error.URLError, subprocess.TimeoutExpired) as error:
        # Error responses can contain credentials; print only a safe classification.
        print("capacity recovery failed: " + type(error).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
