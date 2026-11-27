# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0

"""Bootstrap must use the local API while Kubernetes readiness is false."""

import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shlex
import shutil
import subprocess
import tempfile
import time
import unittest

REPO = Path(__file__).resolve().parents[3]
CHART = REPO / "deploy/helm/openbao/helm/scripts"

FAKE_KUBECTL = r'''#!/usr/bin/env python3
import base64, json, os, re, sys, time
from pathlib import Path
p = Path(os.environ['BOOTSTRAP_TEST_STATE'])
s = json.loads(p.read_text())
a = sys.argv[1:]
s['calls'].append(a)
p.write_text(json.dumps(s))
def done(out='', rc=0):
 p.write_text(json.dumps(s)); print(out, end=''); sys.exit(rc)
def stall(operation):
 p.with_suffix('.stall').write_text(operation)
 time.sleep(10)
if any(x.startswith('--request-timeout=') for x in a): done('', 1)
if 'version' in a: done(json.dumps({'serverVersion':{'gitVersion':'v1.36.6'}}))
if 'containerStatuses' in ' '.join(a): done('false')
if 'statefulset' in a:
 if s.get('stall') == 'get': stall('get')
 if 'member_clock' in s and s.get('job_reads'): p.with_suffix('.clock').write_text(str(s['member_clock']))
 count = s.get('replicas', 3)
 if s.get('scale_after_snapshot'):
  s['replicas'] = s.pop('scale_after_snapshot')
 done(str(count))
if 'exec' in a and 'status' in a:
 name = next(x for x in a if x.startswith('bao-') and x[4:].isdigit())
 i = int(name.rsplit('-', 1)[1])
 if s.get('unavailable', 0):
  s['unavailable'] -= 1; done('', 1)
 if s.get('stall') == 'exec': stall('exec')
 status = dict(s['members'][i])
 counts = s.setdefault('status_calls', {})
 counts[str(i)] = counts.get(str(i), 0) + 1
 if s.get('reseal_primary') and i == 1 and counts[str(i)] == 2:
  s['members'][0]['sealed'] = True
  s['members'][1]['sealed'] = False
 if s.get('bad_status'): done('{"initialized":"false","sealed":true}', 2)
 done(json.dumps(status), 2 if status['sealed'] else 0)
if 'exec' in a and 'init' in a:
 if s.get('stall') == 'init': stall('init')
 s['init_calls'] += 1
 if s.get('deny_init'): done('', 1)
 if s['members'][0]['initialized']: done('', 1)
 s['members'][0]['initialized'] = True
 done(json.dumps({'unseal_keys_b64':['test-key'], 'root_token':'test-root', 'recovery_keys_b64':['test-recovery']}))
if 'exec' in a and 'printenv' in a: done('10.0.0.1')
if 'exec' in a and 'cat' in a:
 if a[a.index('exec')+1] != 'bao-0': done('', 1)
 done('test-service-token')
if 'exec' in a and 'curl' in a:
 if 'Authorization: Bearer test-service-token' not in a: done('', 1)
 done(json.dumps({'keys':[{'kty':'RSA','n':'test','e':'AQAB'}]}))
if 'job' in a and 'get' in a:
 if s.get('job_read_delay'): time.sleep(s['job_read_delay'])
 s['job_reads'] = s.get('job_reads', 0) + 1
 if 'job_clock' in s: p.with_suffix('.clock').write_text(str(s['job_clock']))
 if s.get('job_delay', 0):
  s['job_delay'] -= 1; done(json.dumps({'status':{}}))
 condition = s.get('job_state', 'Complete')
 done(json.dumps({'spec':{'activeDeadlineSeconds':s.get('job_active_deadline',4000)}, 'status':{'startTime':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),'conditions':[{'type':condition, 'status':'True'}]}}))
if 'secret' in a and 'get' in a:
 name = a[a.index('secret') + 1]
 failures = s.get('read_failures', {})
 if failures.get(name, 0):
  failures[name] -= 1; done('ServiceUnavailable', 1)
 if name.endswith('-root-token') and s.get('root_delay', 0):
  s['root_delay'] -= 1
  if not s['root_delay']: s['secrets'][name] = {'root_token':'test-root'}
 if 'secret_read_clock' in s: p.with_suffix('.clock').write_text(str(s['secret_read_clock']))
 if s.get('secret_read_delay'):
  p.with_suffix('.secret-read').write_text(name)
  time.sleep(s['secret_read_delay'])
 data = s['secrets'].get(name)
 if data is None: done('', 0 if '--ignore-not-found' in a else 1)
 if a[-1] == 'name': done(name)
 version = s.setdefault('secret_versions', {}).setdefault(name, 1)
 encoded = {k:base64.b64encode(v.encode()).decode() for k,v in data.items()}
 if a[-1] == 'jsonpath={.metadata.resourceVersion}': done(str(version))
 if a[-1] == 'json': done(json.dumps({'metadata':{'name':name, 'uid':s.get('secret_uids', {}).get(name, name+'-uid'), 'resourceVersion':str(version)}, 'data':encoded}))
 match = re.fullmatch(r'jsonpath=\{\.data\.([^}]+)\}', a[-1])
 if match: done(encoded.get(match[1], ''))
 done('', 1)
if 'secret' in a and ('patch' in a or 'create' in a):
 s['writes'] += 1
 if s.get('fail_write'): done('', 1)
 name = a[a.index('generic')+1] if 'create' in a else a[a.index('secret')+1]
 failures = s.get('write_failures', {})
 if failures.get(name, 0):
  failures[name] -= 1; done('', 1)
 versions = s.setdefault('secret_versions', {})
 if 'patch' in a:
  patch = json.loads(a[a.index('--patch') + 1])
  if s.get('fail_verification') and base64.b64decode(patch['data'].get('bootstrap_verified', '')).decode() == 'true': done('', 1)
  if s.get('restore_before_patch') and name.endswith('-unseal'):
   s['restore_before_patch'] = False
   s['secrets'][name]['unseal_key'] = 'restored-key'
   versions[name] = versions.get(name, 1) + 1
  if s.get('restore_root_before_complete') and 'bootstrap_state' in patch['data']:
   s['restore_root_before_complete'] = False
   s['secrets'][name]['root_token'] = 'restored-root'
   versions[name] = versions.get(name, 1) + 1
  if s.get('verify_before_reset') and name.endswith('-root-token') and base64.b64decode(patch['data'].get('bootstrap_state', '')).decode() == 'pending':
   s['verify_before_reset'] = False
   s['secrets'][name]['bootstrap_verified'] = 'true'
   s['secrets'].pop('bao-recovery-keys', None)
   versions[name] = versions.get(name, 1) + 1
  if s.get('competing_unverified_completion') and name.endswith('-root-token') and base64.b64decode(patch['data'].get('bootstrap_state', '')).decode() == 'complete':
   s['competing_unverified_completion'] = False
   s['secrets'][name]['bootstrap_state'] = 'complete'
   versions[name] = versions.get(name, 1) + 1
   versions['bao-recovery-keys'] = versions.get('bao-recovery-keys', 1) + 1
  if name not in s['secrets']: done('', 1)
  supplied = patch.get('metadata', {}).get('resourceVersion')
  if supplied is not None and supplied != str(versions.get(name, 1)): done('Conflict', 1)
  if s.get('secondary_change_at_completion') and name.endswith('-root-token') and base64.b64decode(patch['data'].get('bootstrap_state', '')).decode() == 'complete':
   change = s.pop('secondary_change_at_completion')
   secondary = 'bao-recovery-keys'
   if change == 'delete': del s['secrets'][secondary]
   elif change == 'replace': s['secrets'][secondary]['recovery_keys_b64'] = '["foreign-recovery"]'
   else: s.setdefault('secret_uids', {})[secondary] = 'replacement-uid'
   versions[secondary] = versions.get(secondary, 1) + 1 if change != 'recreate' else 1
  s['secrets'][name].update({k:base64.b64decode(v).decode() for k,v in patch['data'].items()})
 else:
  if s.get('restore_before_create') and name.endswith('-recovery-keys'):
   s['restore_before_create'] = False
   s['secrets'][name] = {'recovery_keys_b64':'["foreign-recovery"]'}
  if name in s['secrets']: done('', 1)
  # Split field=value after the flag, preserving '=' in the literal value.
  s['secrets'][name] = dict(x[len('--from-literal='):].split('=', 1) for x in a if x.startswith('--from-literal='))
  for arg in a:
   if arg.startswith('--from-file='):
    key, filename = arg[len('--from-file='):].split('=', 1)
    s['secrets'][name][key] = Path(filename).read_text()
 versions[name] = versions.get(name, 0) + 1
 if s.get('lost_write_response'): done('', 1)
 if 'patch' in a and a[-1] == 'json':
  data = {k:base64.b64encode(v.encode()).decode() for k,v in s['secrets'][name].items()}
  done(json.dumps({'metadata':{'name':name, 'uid':name+'-uid', 'resourceVersion':str(versions[name])},'data':data}))
 done()
if 'exec' in a and 'unseal' in a:
 name = next(x for x in a if x.startswith('bao-') and x[4:].isdigit())
 if a[-1] != 'test-key': done('invalid unseal key', 1)
 s['members'][int(name.rsplit('-', 1)[1])]['sealed'] = False
 s['unseal_calls'].append(name); done()
if 'exec' in a and 'join' in a:
 name = next(x for x in a if x.startswith('bao-') and x[4:].isdigit())
 s['join_calls'].append(name)
 if s.get('join_failures', 0):
  s['join_failures'] -= 1; done('', 1)
 s['members'][int(name.rsplit('-', 1)[1])]['initialized'] = True
 if s.get('lost_join_response'): done('', 1)
 done()
done('', 1)
'''


class BootstrapReadinessTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        (self.path / "kubectl").write_text(FAKE_KUBECTL)
        (self.path / "kubectl").chmod(0o755)
        (self.path / "sleep").write_text("#!/bin/sh\n/bin/sleep 0.01\n")
        (self.path / "sleep").chmod(0o755)
        self.state = {
            "replicas": 3,
            "members": [dict(initialized=False, sealed=True, recovery_seal=False) for _ in range(3)],
            "secrets": {"bao-unseal": {"unseal_key": ""}}, "calls": [], "writes": 0,
            "init_calls": 0, "unseal_calls": [], "join_calls": [],
        }
        self.state_file = self.path / "state.json"

    def invoke(self, function="initialize_cluster", standalone=False, success=True, api_timeout="5", mode="shamir",
               credential_timeout="120", arguments="test bao", process_timeout=8, bootstrap_source=None, bootstrap_job="", bootstrap_timeout=None, extra_preamble="", omit_digest_tools=False):
        self.state_file.write_text(json.dumps(self.state))
        source = REPO / "deploy/helm/openbao/deploy.sh" if standalone else CHART / "deploy.sh"
        content = source.read_text()
        body = "" if re.search(r"^" + function + r"\(\)", (CHART / "utils/bootstrap.sh").read_text(), re.M) else re.search(
            r"^" + function + r"\(\) \{.*?^\}", content, re.M | re.S)[0]
        if function == "get_and_save_jwt_signing_key":
            body = body.replace("/tmp/cluster.jwks", str(self.path / "cluster.jwks")).replace("/tmp/jwt.pem", str(self.path / "jwt.pem"))
        helper = bootstrap_source or CHART / "utils/bootstrap.sh"
        preamble = '\n'.join([
            'log_info() { :; }; log_error() { echo "$*" >&2; }; log_success() { :; }; log_warn() { :; }; log_section() { :; }',
            f'resolve_seal_mode() {{ RESOLVED_SEAL_MODE={mode}; }}',
            'check_sidecar_version() { return 0; }',
            re.search(r"^get_root_token\(\) \{.*?^\}", content, re.M | re.S)[0],
            re.search(r"^get_unseal_key\(\) \{.*?^\}", content, re.M | re.S)[0],
            f'source "{helper}"' if helper.exists() else '',
            'OPENBAO_REPLICAS=3', body, extra_preamble, f'{function} {arguments}',
        ])
        env = dict(os.environ, PATH=str(self.path) + os.pathsep + os.environ["PATH"],
                   BOOTSTRAP_TEST_STATE=str(self.state_file), OPENBAO_API_TIMEOUT_SECONDS=api_timeout,
                   OPENBAO_CREDENTIAL_TIMEOUT_SECONDS=credential_timeout, OPENBAO_BOOTSTRAP_JOB_NAME=bootstrap_job,
                   OPENBAO_BOOTSTRAP_TIMEOUT_SECONDS=api_timeout if bootstrap_timeout is None else bootstrap_timeout)
        if omit_digest_tools:
            for name in ("bash", "jq", "python3", "base64", "tr"):
                (self.path / name).symlink_to(shutil.which(name))
            env["PATH"] = str(self.path)
        process = subprocess.Popen(["bash", "-c", preamble], env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=process_timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            self.fail("bootstrap waited for Kubernetes Ready instead of the local API")
        self.state = json.loads(self.state_file.read_text())
        if success:
            self.assertEqual(process.returncode, 0, stderr + stdout)
        else:
            self.assertNotEqual(process.returncode, 0, stdout)
        return stderr

    @staticmethod
    def root_secret(root="test-root", state="complete", mode="shamir", verified=True):
        value = '["test-recovery"]' if mode == "auto" else "test-key"
        result = {"root_token": root, "bootstrap_state": state,
                  "bootstrap_credential_digest": hashlib.sha256(f"{mode}:{value}".encode()).hexdigest()}
        if state == "complete":
            result["bootstrap_verified"] = str(verified).lower()
        return result

    def test_uninitialized_not_ready_members_can_be_initialized(self):
        self.invoke()
        self.assertEqual(self.state["init_calls"], 1)
        self.assertEqual(self.state["secrets"], {"bao-unseal": {"unseal_key": "test-key"}, "bao-root-token": self.root_secret()})
        self.assertFalse(any("containerStatuses" in " ".join(c) for c in self.state["calls"]))

    def test_standalone_initializes_members_without_readiness(self):
        self.invoke(standalone=True)
        self.assertEqual(self.state["init_calls"], 1)
        self.assertEqual(self.state["secrets"], {"bao-unseal": {"unseal_key": "test-key"}, "bao-root-token": self.root_secret()})
        self.assertFalse(any("containerStatuses" in " ".join(c) for c in self.state["calls"]))

    def test_initialized_members_reuse_saved_credentials(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"].update({"bao-root-token": {"root_token": "test-root"}, "bao-unseal": {"unseal_key": "test-key"}})
        self.invoke()
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)

    def test_initialized_without_root_token_fails_without_reinitializing(self):
        self.state["members"][0]["initialized"] = True
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 0)

    def test_uninitialized_with_stale_credentials_fails_closed(self):
        self.state["secrets"]["bao-root-token"] = {"root_token": "old-root"}
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 0)

    def test_retry_after_failed_key_write_does_not_initialize_twice(self):
        self.state["fail_write"] = True
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 1)
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 1)

    def test_healthy_members_are_not_rejoined_or_unsealed(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"]["bao-unseal"] = {"unseal_key": "test-key"}
        self.invoke(function="unseal_cluster")
        self.assertEqual(self.state["unseal_calls"], [])
        self.assertEqual(self.state["join_calls"], [])

    def test_partially_joined_cluster_resumes_without_rejoining_existing_member(self):
        self.state["members"][0].update(initialized=True, sealed=False)
        self.state["members"][2]["initialized"] = True
        self.state["secrets"]["bao-unseal"] = {"unseal_key": "test-key"}
        self.invoke(function="unseal_cluster")
        self.assertEqual(self.state["join_calls"], ["bao-1"])
        self.assertEqual(self.state["unseal_calls"], ["bao-1", "bao-2"])

    def test_transient_api_failure_is_retried(self):
        self.state["unavailable"] = 1
        self.invoke()
        self.assertEqual(self.state["init_calls"], 1)

    def test_unavailable_api_times_out_without_writes(self):
        self.state["unavailable"] = 10000
        self.invoke(success=False, api_timeout="1")
        self.assertEqual(self.state["writes"], 0)
        self.assertEqual(self.state["init_calls"], 0)

    def test_malformed_status_times_out_without_writes(self):
        self.state["bad_status"] = True
        self.invoke(success=False, api_timeout="1")
        self.assertEqual(self.state["writes"], 0)
        self.assertEqual(self.state["init_calls"], 0)

    def test_live_replica_count_is_used(self):
        self.state["replicas"] = 2
        self.state["members"] = self.state["members"][:2]
        self.invoke()
        self.assertFalse(any("bao-2" in call for call in self.state["calls"]))

    def test_missing_unseal_secret_fails_before_initialization(self):
        self.state["secrets"].clear()
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 0)

    def test_stale_recovery_keys_fail_before_initialization(self):
        self.state["secrets"]["bao-recovery-keys"] = {"recovery_keys_b64": "old-recovery"}
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 0)

    def test_post_install_waits_without_initializing_or_writing_credentials(self):
        self.invoke(function="wait_for_openbao_bootstrap", success=False, api_timeout="1")
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)

    def test_post_install_continues_after_all_members_are_unsealed(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        self.invoke(function="wait_for_openbao_bootstrap")
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)

    def test_initialized_shamir_without_unseal_key_fails_closed(self):
        self.state["members"][0]["initialized"] = True
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 0)

    def test_initialized_auto_unseal_reuses_root_without_shamir_key(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False, recovery_seal=True)
        self.state["secrets"] = {"bao-root-token": {"root_token": "test-root"}}
        self.invoke(mode="auto")
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)

    def test_auto_unseal_wait_does_not_join_or_read_shamir_key(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False, recovery_seal=True)
        self.state["secrets"].clear()
        self.invoke(function="unseal_cluster", mode="auto")
        self.assertEqual(self.state["join_calls"], [])
        self.assertEqual(self.state["unseal_calls"], [])
        self.assertFalse(any("secret" in c for c in self.state["calls"]))

    def test_auto_unseal_wait_times_out_without_manual_unseal(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=True, recovery_seal=True)
        self.invoke(function="unseal_cluster", mode="auto", success=False, api_timeout="1")
        self.assertEqual(self.state["join_calls"], [])
        self.assertEqual(self.state["unseal_calls"], [])

    def test_stale_auto_recovery_credentials_fail_before_initialization(self):
        self.state["secrets"] = {"bao-recovery-keys": {"recovery_keys_b64": "old-recovery"}}
        self.invoke(mode="auto", success=False)
        self.assertEqual(self.state["init_calls"], 0)

    def test_invalid_api_timeout_fails_before_calling_kubernetes(self):
        for value in ("0", "", "-1", "invalid"):
            with self.subTest(value=value):
                self.invoke(success=False, api_timeout=value)
                self.assertEqual(self.state["calls"], [])

    def run_script(self, standalone=False, process_timeout=8, **overrides):
        stage = self.path / "stage"
        utils = stage / "utils"
        utils.mkdir(parents=True, exist_ok=True)
        source = REPO / "deploy/helm/openbao" if standalone else CHART
        shutil.copyfile(source / "deploy.sh", stage / "deploy.sh")
        for name in ["utils.sh", "log.sh"]:
            shutil.copyfile(CHART / name, utils / name)
        if standalone:
            utils = stage / "helm/scripts/utils"
            utils.mkdir(parents=True, exist_ok=True)
        for name in ["jwt-plugin.sh", "bootstrap.sh"]:
            shutil.copyfile(CHART / "utils" / name, utils / name)
        jwker = self.path / "jwker"
        jwker.write_text("#!/bin/sh\nexit 0\n")
        jwker.chmod(0o755)
        self.state_file.write_text(json.dumps(self.state))
        env = dict(os.environ, PATH=str(self.path) + os.pathsep + os.environ["PATH"],
                   BOOTSTRAP_TEST_STATE=str(self.state_file), OPENBAO_API_TIMEOUT_SECONDS="5",
                   OPENBAO_BOOTSTRAP_ONLY="false", OPENBAO_WAIT_ONLY="false")
        env.update(overrides)
        result = subprocess.run(["bash", str(stage / "deploy.sh"), "test", "bao", "helm"],
                                env=env, capture_output=True, text=True, timeout=process_timeout)
        self.state = json.loads(self.state_file.read_text())
        return result

    def test_post_install_script_never_attempts_initialization(self):
        self.state["deny_init"] = True
        result = self.run_script()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)
        self.assertTrue(any("status" in call for call in self.state["calls"]))
        self.assertIn("Timed out waiting for OpenBao bootstrap to finish", result.stdout + result.stderr)

    def test_standalone_existing_resources_invocation_resumes_bootstrap(self):
        self.state["members"][0].update(initialized=True, sealed=False)
        self.state["secrets"].update({"bao-root-token": {"root_token": "test-root"}, "bao-unseal": {"unseal_key": "test-key"}})
        result = self.run_script(standalone=True, process_timeout=30, OPENBAO_API_TIMEOUT_SECONDS="20")
        self.assertIn("reusing saved bootstrap credentials", result.stdout)
        self.assertIn("Successfully unsealed the cluster", result.stdout)
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)
        self.assertEqual(self.state["join_calls"], ["bao-1", "bao-2"])

    def test_post_upgrade_wait_only_skips_jwt_and_initialization(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        result = self.run_script(OPENBAO_WAIT_ONLY="true", OPENBAO_API_TIMEOUT_SECONDS="5")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)
        self.assertFalse(any("printenv" in call for call in self.state["calls"]))

    def test_transient_credential_write_failures_are_retried_in_both_scripts(self):
        for standalone in (False, True):
            with self.subTest(standalone=standalone):
                self.state["members"][0]["initialized"] = False
                self.state["secrets"] = {"bao-unseal": {"unseal_key": ""}}
                self.state["write_failures"] = {"bao-unseal": 1, "bao-root-token": 1}
                self.invoke(standalone=standalone)
                self.assertEqual(self.state["secrets"], {"bao-unseal": {"unseal_key": "test-key"}, "bao-root-token": self.root_secret()})
        self.assertEqual(self.state["init_calls"], 2)

    def test_lost_write_responses_are_verified_without_reinitializing(self):
        self.state["lost_write_response"] = True
        self.invoke()
        self.assertEqual(self.state["init_calls"], 1)
        self.assertEqual(self.state["writes"], 4)
        self.assertEqual(self.state["secrets"], {"bao-unseal": {"unseal_key": "test-key"}, "bao-root-token": self.root_secret()})

    def test_root_is_saved_even_if_key_writes_keep_failing(self):
        self.state["write_failures"] = {"bao-unseal": 100}
        self.invoke(success=False)
        self.assertEqual(self.state["init_calls"], 1)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["root_token"], "test-root")

    def test_auto_seal_persists_recovery_keys_and_root(self):
        self.state["secrets"].clear()
        self.invoke(mode="auto")
        self.assertEqual(json.loads(self.state["secrets"]["bao-recovery-keys"]["recovery_keys_b64"]), ["test-recovery"])
        self.assertEqual(self.state["secrets"]["bao-root-token"]["root_token"], "test-root")

    def test_post_install_waits_for_saved_root_token(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.invoke(function="wait_for_openbao_bootstrap", success=False, api_timeout="1")
        self.state["root_delay"] = 2
        self.invoke(function="wait_for_openbao_bootstrap")
        self.assertEqual(self.state["root_delay"], 0)
        self.assertEqual(self.state["writes"], 0)

    def test_stalled_kubernetes_get_and_exec_are_bounded(self):
        for operation in ("get", "exec"):
            with self.subTest(operation=operation):
                self.state["stall"] = operation
                self.state_file.with_suffix(".stall").unlink(missing_ok=True)
                started = time.monotonic()
                self.invoke(function="wait_for_openbao_api", success=False, api_timeout="3")
                self.assertEqual(self.state_file.with_suffix(".stall").read_text(), operation)
                self.assertLess(time.monotonic() - started, 6)

    def test_join_failures_are_retried_and_lost_response_is_rechecked(self):
        for lost_response in (False, True):
            with self.subTest(lost_response=lost_response):
                for member in self.state["members"]:
                    member.update(initialized=False, sealed=True)
                self.state["members"][0].update(initialized=True, sealed=False)
                self.state["secrets"]["bao-unseal"] = {"unseal_key": "test-key"}
                self.state["join_calls"] = []
                self.state["join_failures"] = 1
                self.state["lost_join_response"] = lost_response
                self.invoke(function="unseal_cluster")
                self.assertEqual(self.state["join_calls"], ["bao-1", "bao-1", "bao-2"])

    def test_persistent_join_failure_times_out(self):
        self.state["members"][0].update(initialized=True, sealed=False)
        self.state["secrets"]["bao-unseal"] = {"unseal_key": "test-key"}
        self.state["join_failures"] = 1000
        self.invoke(function="unseal_cluster", success=False, api_timeout="1")
        self.assertNotIn("bao-2", self.state["join_calls"])

    def test_wrong_unseal_key_is_rejected(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=True)
        self.state["secrets"]["bao-unseal"] = {"unseal_key": "wrong-key"}
        self.invoke(function="unseal_cluster", success=False, api_timeout="5")
        self.assertTrue(any("unseal" in call for call in self.state["calls"]))
        self.assertTrue(all(member["sealed"] for member in self.state["members"]))

    def test_auto_seal_rechecks_primary_while_waiting_for_peer(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["members"][1]["sealed"] = True
        self.state["reseal_primary"] = True
        self.invoke(function="unseal_cluster", mode="auto", success=False, api_timeout="5")
        self.assertTrue(self.state["members"][0]["sealed"])


    def test_slow_secret_reads_use_a_separate_persistence_budget(self):
        self.state["secret_read_delay"] = 6
        self.invoke(function="persist_bootstrap_secret", api_timeout="1", credential_timeout="20",
                    arguments="test bao-unseal unseal_key test-key patch", process_timeout=25)
        self.assertEqual(self.state["secrets"]["bao-unseal"]["unseal_key"], "test-key")
        self.assertEqual(self.state["writes"], 1)

    def test_standalone_unseal_allows_slow_credential_read_within_phase_budget(self):
        self.state["secret_read_delay"] = 6
        self.state["secrets"]["bao-unseal"] = {"unseal_key": "test-key"}
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.invoke(function="unseal_cluster", standalone=True, api_timeout="15", process_timeout=20)
        self.assertTrue(self.state_file.with_suffix(".secret-read").exists())

    def test_completion_retries_transient_secret_reads(self):
        for name in ("bao-root-token", "bao-recovery-keys"):
            with self.subTest(name=name):
                self.state["secrets"] = {
                    "bao-root-token": self.root_secret(state="pending", mode="auto"),
                    "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
                }
                self.state["read_failures"] = {name: 1}
                self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root")
                self.assertEqual(self.state["read_failures"][name], 0)
                self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "complete")

    def test_pending_retry_rejects_foreign_recovery_keys_created_during_init(self):
        self.state["secrets"].clear()
        self.state["restore_before_create"] = True
        self.invoke(mode="auto", success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"], self.root_secret(state="pending", mode="auto"))
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False, recovery_seal=True)
        self.invoke(mode="auto", success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")
        self.assertEqual(self.state["init_calls"], 1)

    def test_pending_retry_requires_original_digest(self):
        self.state["secrets"] = {
            "bao-root-token": {"root_token": "test-root", "bootstrap_state": "pending"},
            "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
        }
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root", success=False)
        self.assertEqual(self.state["writes"], 0)

    def test_secondary_delete_replace_and_recreate_revoke_completion(self):
        for change in ("delete", "replace", "recreate"):
            with self.subTest(change=change):
                self.state["secrets"] = {
                    "bao-root-token": self.root_secret(state="pending", mode="auto"),
                    "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
                }
                self.state["secret_versions"] = {}
                self.state["secret_uids"] = {}
                self.state["secondary_change_at_completion"] = change
                self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root", success=False)
                self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")

    def test_later_job_retry_cannot_skip_failed_post_write_verification(self):
        self.state["secrets"] = {
            "bao-root-token": self.root_secret(mode="auto", verified=False),
            "bao-recovery-keys": {"recovery_keys_b64": '["foreign-recovery"]'},
        }
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root",
                    bootstrap_job="bootstrap-2", success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")

    def test_failed_receipt_write_blocks_hooks_and_requires_reverification(self):
        self.state["secrets"].clear()
        self.state["fail_verification"] = True
        self.invoke(mode="auto", success=False, bootstrap_job="bootstrap-1")
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "complete")
        self.assertNotEqual(self.state["secrets"]["bao-root-token"].get("bootstrap_verified"), "true")
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False, recovery_seal=True)
        start = len(self.state["calls"])
        options = dict(function="wait_for_openbao_bootstrap", bootstrap_job="bootstrap-1", api_timeout="3")
        self.invoke(**options, success=False)
        self.assertTrue(any(call[:3] == ["get", "secret", "bao-root-token"] and call[-1] == "json"
                            for call in self.state["calls"][start:]))
        self.state["secrets"]["bao-root-token"]["bootstrap_verified"] = "true"
        self.invoke(**options)
        del self.state["secrets"]["bao-root-token"]["bootstrap_verified"]
        self.state["fail_verification"] = False
        self.state["secrets"]["bao-recovery-keys"]["recovery_keys_b64"] = '["foreign-recovery"]'
        self.invoke(mode="auto", bootstrap_job="bootstrap-2", success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")
        self.assertEqual(self.state["init_calls"], 1)

    def test_hook_waits_for_current_bootstrap_job_and_rejects_failed_job(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        self.state["job_state"] = "Pending"
        self.invoke(function="wait_for_openbao_bootstrap", bootstrap_job="bootstrap-2", api_timeout="2", success=False)
        self.state["job_state"] = "Failed"
        self.state["unavailable"] = 10000
        error = self.invoke(function="wait_for_openbao_bootstrap", bootstrap_job="bootstrap-2", success=False)
        self.assertIn("OpenBao bootstrap Job failed", error)
        self.assertEqual(self.state["unavailable"], 10000)
        self.state["unavailable"] = 0
        self.state["job_state"] = "Complete"
        self.state["job_delay"] = 1
        self.invoke(function="wait_for_openbao_bootstrap", bootstrap_job="bootstrap-2")
        self.assertEqual(self.state["job_delay"], 0)
        self.assertEqual(self.state["writes"], 0)
        self.assertTrue(all(call[call.index("job") + 1] == "bootstrap-2"
                            for call in self.state["calls"] if "job" in call))

    def test_fake_allows_unconditional_merge_and_detects_missing_cas_guards(self):
        mutated = self.path / "bootstrap-without-cas.sh"
        source = (CHART / "utils/bootstrap.sh").read_text()
        mutated.write_text(source.replace('{metadata: {resourceVersion: $version}, data:', '{data:'))
        self.state["restore_before_patch"] = True
        self.invoke(bootstrap_source=mutated)
        self.assertEqual(self.state["secrets"]["bao-unseal"]["unseal_key"], "test-key")
        # Removing only the root CAS must also violate the restored pending state.
        self.state["members"][0]["initialized"] = False
        self.state["secrets"] = {"bao-unseal": {"unseal_key": ""}}
        self.state["restore_root_before_complete"] = True
        self.invoke(bootstrap_source=mutated, success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["root_token"], "restored-root")
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "complete")

    def test_missing_digest_utilities_fail_before_standalone_init(self):
        error = self.invoke(standalone=True, omit_digest_tools=True, success=False)
        self.assertIn("A working sha256sum or shasum is required", error)
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 0)

    def test_recovery_digest_requires_exactly_one_json_array(self):
        for value in ('["test-recovery"] null', '["test-recovery"] false', '["test-recovery"] []'):
            with self.subTest(value=value):
                self.state["secrets"] = {
                    "bao-root-token": self.root_secret(state="pending", mode="auto"),
                    "bao-recovery-keys": {"recovery_keys_b64": value},
                }
                self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root", success=False)
                self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")
        self.state["secrets"]["bao-recovery-keys"]["recovery_keys_b64"] = '[\n  "test-recovery"\n]'
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root")

    def test_stale_reset_preserves_another_attempts_receipt_and_export(self):
        self.state["secrets"] = {
            "bao-root-token": self.root_secret(state="pending", mode="auto"),
            "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
        }
        self.state["secondary_change_at_completion"] = "replace"
        self.state["verify_before_reset"] = True
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root", success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "complete")
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_verified"], "true")
        self.assertNotIn("bao-recovery-keys", self.state["secrets"])
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root")

    def test_credential_reads_do_not_put_secrets_in_jq_arguments(self):
        self.state["secrets"] = {
            "bao-root-token": self.root_secret(state="pending", mode="auto"),
            "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
        }
        guard = '''jq() {
            for arg in "$@"; do
                case "$arg" in *dGVzdC1yb290*|*WyJ0ZXN0LXJlY292ZXJ5Il0=*) return 99;; esac
            done
            command jq "$@"
        }'''
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root", extra_preamble=guard)

    def test_job_wait_has_its_own_budget_before_final_api_checks(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        self.state["job_read_delay"] = 1.5
        self.state["job_delay"] = 1
        self.invoke(function="wait_for_openbao_bootstrap", bootstrap_job="bootstrap-1", api_timeout="2",
                    credential_timeout="1", bootstrap_timeout="6", process_timeout=15)
        self.assertEqual(self.state["job_reads"], 2)

    def test_documented_export_rejects_changed_or_multi_document_shares(self):
        doc = (REPO / "deploy/helm/openbao/README.md").read_text().split("### Recovery-key export", 1)[1]
        command = re.search(r"```bash\n(.*?)```", doc, re.S)[1]
        for shares, accepted in (('["test-recovery"]', True), ('["foreign"]', False), ('["test-recovery"] null', False)):
            with self.subTest(shares=shares):
                self.state["secrets"] = {
                    "bao-root-token": self.root_secret(mode="auto"),
                    "bao-recovery-keys": {"recovery_keys_b64": shares},
                }
                self.state_file.write_text(json.dumps(self.state))
                env = dict(os.environ, PATH=str(self.path) + os.pathsep + os.environ["PATH"],
                           BOOTSTRAP_TEST_STATE=str(self.state_file), KUBE_CONTEXT="test-context",
                           OPENBAO_NAMESPACE="test", OPENBAO_SERVER_NAME="bao", TMPDIR=str(self.path))
                result = subprocess.run(["bash", "-c", command], cwd=REPO, env=env,
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode == 0, accepted, result.stderr)
                self.assertEqual("Verified recovery-key export:" in result.stdout, accepted)
                self.assertNotIn("test-recovery", result.stdout)
                self.assertNotIn("foreign", result.stdout)
                exports = list(self.path.glob("openbao-recovery.*"))
                self.assertEqual(len(exports), 1 if accepted else 0)
                for export in exports:
                    self.assertEqual(export.stat().st_mode & 0o777, 0o600)
                    export.unlink()

    def controlled_clock_source(self):
        # Replace clock reads only; leave timeout environment names intact.
        clock = self.state_file.with_suffix(".clock")
        clock.write_text("0")
        helper = self.path / "bootstrap-clock.sh"
        source = (CHART / "utils/bootstrap.sh").read_text()
        source = re.sub(r"(?<![A-Za-z0-9_])\$?SECONDS(?![A-Za-z0-9_])", "$(bootstrap_test_now)", source)
        helper.write_text(source)
        return helper, f"bootstrap_test_now() {{ cat {shlex.quote(str(clock))}; }}"

    def test_ambiguous_completion_restarts_from_fresh_secret_snapshots(self):
        self.state["secrets"] = {
            "bao-root-token": self.root_secret(state="pending", mode="auto"),
            "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
        }
        self.state["competing_unverified_completion"] = True
        self.invoke(function="complete_bootstrap_credentials", arguments="test bao auto test-root")
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_verified"], "true")
        self.assertEqual(self.state["secret_versions"]["bao-recovery-keys"], 2)

    def test_init_requires_remaining_job_time_in_both_scripts(self):
        original = json.dumps(self.state)
        for standalone in (False, True):
            with self.subTest(standalone=standalone):
                self.state = json.loads(original)
                self.state["job_active_deadline"] = 40
                error = self.invoke(standalone=standalone, bootstrap_job="bootstrap-1", api_timeout="2",
                                    credential_timeout="2", success=False)
                self.assertIn("Not enough bootstrap Job time remains", error)
                self.assertEqual(self.state["init_calls"], 0)
                self.assertEqual(self.state["writes"], 0)
                self.state["job_active_deadline"] = 4000
                self.invoke(standalone=standalone, bootstrap_job="bootstrap-1", api_timeout="2", credential_timeout="2")
                self.assertEqual(self.state["init_calls"], 1)
                self.assertEqual(self.state["secrets"]["bao-root-token"]["root_token"], "test-root")

    def test_stalled_init_request_is_bounded(self):
        self.state["stall"] = "init"
        started = time.monotonic()
        self.invoke(api_timeout="2", success=False)
        self.assertEqual(self.state_file.with_suffix(".stall").read_text(), "init")
        self.assertLess(time.monotonic() - started, 6)
        self.assertEqual(self.state["writes"], 0)

    def test_persistence_cannot_outlive_the_safe_job_deadline(self):
        helper, clock = self.controlled_clock_source()
        self.state["secret_read_clock"] = 8
        self.invoke(function="persist_bootstrap_secret", credential_timeout="20", success=False,
                    arguments="test bao-unseal unseal_key test-key patch", bootstrap_source=helper,
                    extra_preamble=clock + "; bootstrap_job_deadline=7")
        self.assertEqual(self.state["writes"], 0)
        self.assertTrue(any(call[:2] == ["get", "secret"] for call in self.state["calls"]))

    def test_final_member_checks_need_a_fresh_api_deadline(self):
        helper, clock = self.controlled_clock_source()
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        self.state.update(job_clock=5, member_clock=7)
        options = dict(function="wait_for_openbao_bootstrap", bootstrap_job="bootstrap-1", api_timeout="4",
                       bootstrap_timeout="6", extra_preamble=clock)
        self.invoke(**options, bootstrap_source=helper)
        original = helper.read_text()
        mutated = original.replace('        bootstrap_deadline=""\n        bootstrap_start_wait || return 1\n', '')
        self.assertNotEqual(original, mutated)
        helper.write_text(mutated)
        self.state_file.with_suffix(".clock").write_text("0")
        error = self.invoke(**options, bootstrap_source=helper, success=False)
        self.assertIn("Timed out waiting for OpenBao API", error)

    def test_credential_persistence_budget_is_bounded(self):
        self.state["secret_read_delay"] = 10
        started = time.monotonic()
        self.invoke(function="persist_bootstrap_secret", credential_timeout="3", success=False,
                    arguments="test bao-unseal unseal_key test-key patch")
        self.assertEqual(self.state_file.with_suffix(".secret-read").read_text(), "bao-unseal")
        self.assertLess(time.monotonic() - started, 6)
        self.assertEqual(self.state["writes"], 0)

    def test_invalid_credential_timeout_fails_before_init(self):
        for value in ("0", "", "-1", "invalid"):
            with self.subTest(value=value):
                self.invoke(credential_timeout=value, success=False)
                self.assertEqual(self.state["init_calls"], 0)

    def test_secret_compare_and_swap_preserves_a_concurrent_restore(self):
        self.state["restore_before_patch"] = True
        self.invoke(success=False)
        self.assertEqual(self.state["secrets"]["bao-unseal"]["unseal_key"], "restored-key")
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")
        self.assertEqual(self.state["init_calls"], 1)

    def test_completion_does_not_accept_a_concurrently_changed_root(self):
        self.state["restore_root_before_complete"] = True
        self.invoke(success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"],
                         self.root_secret(root="restored-root", state="pending"))

    def test_auto_seal_missing_recovery_keys_blocks_retry_and_hook(self):
        self.state["secrets"].clear()
        self.state["write_failures"] = {"bao-recovery-keys": 100}
        self.invoke(mode="auto", success=False)
        self.assertEqual(self.state["secrets"]["bao-root-token"],
                         self.root_secret(state="pending", mode="auto"))
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False, recovery_seal=True)
        self.invoke(mode="auto", success=False)
        self.invoke(function="wait_for_openbao_bootstrap", success=False, api_timeout="3")
        self.assertEqual(self.state["init_calls"], 1)
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "pending")

    def test_pending_credentials_complete_on_retry_and_allow_recovery_export(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False, recovery_seal=True)
        self.state["secrets"] = {
            "bao-root-token": self.root_secret(state="pending", mode="auto"),
            "bao-recovery-keys": {"recovery_keys_b64": '["test-recovery"]'},
        }
        self.invoke(mode="auto", bootstrap_job="bootstrap-1")
        self.assertEqual(self.state["secrets"]["bao-root-token"]["bootstrap_state"], "complete")
        del self.state["secrets"]["bao-recovery-keys"]
        self.invoke(mode="auto", bootstrap_job="bootstrap-2")
        self.invoke(function="wait_for_openbao_bootstrap")
        self.assertEqual(self.state["init_calls"], 0)
        self.assertEqual(self.state["writes"], 2)

    def test_hook_checks_members_added_after_the_initial_snapshot(self):
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.state["members"][2]["sealed"] = True
        self.state["replicas"] = 2
        self.state["scale_after_snapshot"] = 3
        self.state["secrets"]["bao-root-token"] = {"root_token": "test-root"}
        self.invoke(function="wait_for_openbao_bootstrap", success=False, api_timeout="3")
        self.assertTrue(any("bao-2" in call for call in self.state["calls"]))
        self.state["members"][2]["sealed"] = False
        self.invoke(function="wait_for_openbao_bootstrap")

    def test_fake_preserves_field_names(self):
        self.state["secrets"]["bao-root-token"] = {"rootToken": "test-root"}
        for member in self.state["members"]:
            member.update(initialized=True, sealed=False)
        self.invoke(success=False)
        self.invoke(function="wait_for_openbao_bootstrap", success=False, api_timeout="3")
        self.assertEqual(self.state["init_calls"], 0)

    def test_standalone_jwt_recovery_uses_the_supplied_statefulset(self):
        jwker = self.path / "jwker"
        jwker.write_text("#!/bin/sh\necho test-public-key\n")
        jwker.chmod(0o755)
        self.invoke(function="get_and_save_jwt_signing_key", standalone=True)
        execs = [call for call in self.state["calls"] if "exec" in call]
        self.assertTrue(any("cat" in call for call in execs))
        self.assertTrue(all(call[call.index("exec") + 1] == "bao-0" for call in execs))


class UnsealMonitorTest(unittest.TestCase):
    def test_monitor_retries_lost_process_and_confirms_stopped_state(self):
        for never_stops in (False, True):
            with self.subTest(never_stops=never_stops), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp)
                for pid in (101, 102):
                    proc = path / "proc" / str(pid)
                    proc.mkdir(parents=True)
                    (proc / "comm").write_text("sleep\n")
                    (proc / "status").write_text("PPid: 1\nState: S\n")
                fake = path / "kubectl"
                fake.write_text(r'''#!/usr/bin/env python3
import os, subprocess, sys
from pathlib import Path
root = Path(os.environ['MONITOR_TEST_DIR'])
a = sys.argv[1:]
script = a[a.index('--') + 3].replace('/proc/', str(root / 'proc') + '/')
prefix = r"""
kill() {
    if test "$1" = -STOP; then
        printf '%s\n' "$2" >> "$MONITOR_TEST_DIR/signals"
        if test "$2" = 101; then return 1; fi
        if test "$MONITOR_NEVER_STOPS" = false; then
            printf 'PPid: 1\nState: T\n' > "$MONITOR_TEST_DIR/proc/$2/status"
        fi
    fi
}
sleep() {
    if test "$1" = 240; then : > "$MONITOR_TEST_DIR/watchdog"; fi
}
"""
sys.exit(subprocess.run(['sh', '-ec', prefix + script, 'qa-monitor', a[-1]]).returncode)
''')
                fake.chmod(0o755)
                env = dict(os.environ, PATH=str(path) + os.pathsep + os.environ["PATH"],
                           MONITOR_TEST_DIR=tmp, MONITOR_NEVER_STOPS=str(never_stops).lower())
                result = subprocess.run(["bash", str(REPO / "tests/bdd/scripts/openbao-unseal-monitor.sh"),
                                         "test-context", "test-namespace", "test-pod", "pause"],
                                        env=env, capture_output=True, text=True, timeout=5)
                signals = (path / "signals").read_text().splitlines()
                self.assertEqual(signals[:2], ["101", "102"])
                if never_stops:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("monitor=pause", result.stdout)
                    self.assertFalse((path / "watchdog").exists())
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("monitor=pause", result.stdout)


class BootstrapFeatureAssertionsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="openbao qa ")
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        fake = self.path / "kubectl"
        fake.write_text('''#!/usr/bin/env python3
import json, os, sys
if 'endpointslices' in sys.argv:
 print(os.environ['FEATURE_ENDPOINTS'])
elif 'secret' in sys.argv:
 print(os.environ['FEATURE_BOOTSTRAP_STATE'], end='')
 print(os.environ.get('FEATURE_STDERR', ''), file=sys.stderr)
elif 'secrets' in sys.argv:
 print(json.dumps({'items': []}))
else:
 print(os.environ['FEATURE_STATUS'])
''')
        fake.chmod(0o755)
        (self.path / "sealed-member").write_text("openbao-server-0")
        self.feature = (REPO / "tests/bdd/features/openbao-bootstrap-readiness.feature").read_text()

    def run_feature_command(self, match, **inputs):
        line = next(line.strip() for line in self.feature.splitlines()
                    if line.strip().startswith("/bin/bash") and match in line)
        code = shlex.split(line)[5]
        env = dict(os.environ, PATH=str(self.path) + os.pathsep + os.environ["PATH"], **inputs)
        return subprocess.run(["bash", "-e", "-o", "pipefail", "-c", code,
                               "qa", "test-context", "test-namespace", str(self.path)],
                              env=env, capture_output=True, text=True, timeout=5)

    def test_both_cluster_snapshots_reject_missing_or_invalid_ids(self):
        # Match the invalid baseline and credential hash so cmp cannot hide a
        # missing validation in the after-snapshot command.
        credential_digest = hashlib.sha256(b"[]\n").hexdigest() + "  -\n"
        for status, baseline in (({}, "null\n"), ({"cluster_id": ""}, "\n"), ({"cluster_id": 123}, "123\n")):
            for snapshot in ("cluster-id-before", "cluster-id-after"):
                with self.subTest(status=status, snapshot=snapshot):
                    (self.path / "cluster-id-before").write_text(baseline)
                    (self.path / "credentials-before").write_text(credential_digest)
                    result = self.run_feature_command(snapshot, FEATURE_STATUS=json.dumps(status))
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotIn("state=preserved", result.stdout)
        for snapshot in ("cluster-id-before", "cluster-id-after"):
            result = self.run_feature_command(snapshot, FEATURE_STATUS='{"cluster_id":"test-cluster"}')
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_completion_assertion_rejects_every_state_except_complete(self):
        import base64
        for state in ("pending", "incomplete", "", "complete\n", "complete"):
            with self.subTest(state=state):
                result = self.run_feature_command("bootstrap_state", FEATURE_BOOTSTRAP_STATE=base64.b64encode(state.encode()).decode(),
                                                  FEATURE_STDERR="complete")
                self.assertEqual(result.returncode == 0, state == "complete", result.stderr)

    def test_exclusion_requires_another_ready_openbao_member(self):
        def endpoint(name, ready):
            return {"targetRef": {"name": name}, "conditions": {"ready": ready}}
        for endpoints, accepted in [
            ([], False),
            ([endpoint("openbao-server-0", False)], False),
            ([endpoint("openbao-server-0", True), endpoint("openbao-server-1", True)], False),
            ([endpoint("openbao-server-0", False), endpoint("unrelated-pod", True)], False),
            ([endpoint("openbao-server-0", False), endpoint("openbao-server-1", True)], True),
        ]:
            with self.subTest(endpoints=endpoints):
                result = self.run_feature_command("endpoint=excluded", FEATURE_ENDPOINTS=json.dumps({"items": [{"endpoints": endpoints}]}))
                self.assertEqual(result.returncode == 0, accepted, result.stderr)
