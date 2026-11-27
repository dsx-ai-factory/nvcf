# Manual rehearsal measurement

This is reference material for future execution qualification, outside Phase 1 acceptance.

Use this procedure with the [manual reference](../../../../docs/dev/upgrade-transitions.md)
and the selected route contract. It defines a repeatable measurement. It does
not qualify either route or supply an automated upgrade runner.

## Freeze the comparison

Before either run, record the route, contract and artifact-lock hashes, exact
product manifest and archive digests, stack commits, chart/image digests, tool versions,
site-value hashes and enabled features. Record the existing topology, storage,
registrations and workload fixture identities. Use separate private run
directories and equivalent restored source cohorts for the manual and automated
runs. Do not reuse a partially upgraded cohort as the other baseline.

Use the same workload mix, submission rate, caller-pause policy and acceptance
criteria. Keep physical GPU and cross-host failure claims outside a fake-GPU,
single-host rehearsal. Record registry retries and artifact downloads separately
from deployment execution; a warm cache is a different input.

## Capture the work

Record UTC timestamps and a monotonic duration for each operator activity:

```csv
event_id,action_id,event_type,start_utc,end_utc,elapsed_seconds,result,evidence_sha256
```

Use these event types:

| Type | Count and duration |
| --- | --- |
| `command` | Each command the operator enters, including polls and retries |
| `file-edit` | Each intentional edit to configuration or an execution input |
| `decision` | Each required operator choice, including conflict resolution |
| `recovery-intervention` | Each operator action needed after a failure |
| `hands-on` | Active operator work; exclude unattended waiting |
| `caller-pause` | The interval between stopping and allowing new submissions |
| `availability-probe` | Request start/end, protocol, result and timeout |

Keep command arguments, raw output and secret-bearing inputs private. The public
record contains counts, durations, result categories and evidence hashes. Count
the native commands within a wrapper separately from operator-entered commands.
A wrapper reducing entered commands does not establish reduced decisions or
hands-on work. Mark automated commands explicitly.

1. Start measurement before inventory, source acceptance and prerequisite checks.
2. Record caller pause, native task completion, queue drain and writer pause as
   distinct events. Waiting for a session's native TTL is unattended elapsed
   time unless an operator is actively diagnosing it. Never purge or cancel
   work to shorten that interval.
3. Record the source data gate, consistent backup and isolated restore. A later
   fresh backup is a separate artifact; an earlier restore does not prove that
   this later snapshot was restored.
4. Record every native dependency, OpenBao, core, compute and Observability
   action in contract order. Save migration Job UIDs, actual images, live data
   checks and member replacement results. Keep the original registration files.
5. At the first compute boundary, stop the orchestration process after its
   successful checkpoint. Resume in a separate process after rechecking inputs,
   installed version, original backend identity and mixed-fleet acceptance.
   Record whether the completed compute action was reapplied. This tests an
   action boundary; it does not test an interrupted migration or command.
6. Record final old/new workload, secret, credential, topology and data checks
   before the caller-resume checkpoint. Include recovery and retries in totals.

## Report the result

For each action, report elapsed time, active hands-on time, entered commands,
native commands, file edits, decisions and recovery interventions. Compute
hands-on time from the union of active intervals so parallel work is not counted
twice. Report per-route elapsed time separately from wall time across both
parallel routes. Label shared operator work explicitly.

Report attempted, successful, failed and timed-out invocations by protocol and
action. Record sampling intervals and the longest observed failed-call interval.
Exclude deliberately paused callers from the invocation-success denominator,
but report their pause duration. A period without probes is unobserved, not
available. Zero failed sampled calls does not prove zero downtime.

Compare manual and automated runs only when their frozen inputs and acceptance
criteria match. For a nonzero manual baseline, calculate reduction as
`(manual - automated) / manual`. Keep an unmeasured value `null`; do not infer
hands-on time from process duration or claim a reduction from command batching.
Separate the remaining external pause and irreversible-boundary decisions from
routine work that automation removed. Record failed runs and qualification
limits alongside the comparison.
