# Recovery prerequisites

This is reference material for future execution qualification, outside Phase 1 acceptance.

Keep source recovery requirements separate from a successful chart apply,
component snapshot or Pod restart. Both transitions remain unqualified.

## Pause before the backup

The action order is baseline, caller pause and drain, affected-writer pause,
source data validation, then consistent backup and isolated restore. Preserve
the verified data epoch until the backup is complete. Every disruptive hop
action depends on that recovery gate.

Do not purge requests, cancel tasks or remove workloads to satisfy the drain.
Record failed fixtures and their native terminal states. A task still waiting
for its native timeout blocks the affected drain.

## NATS memory storage

The first source runs NATS 2.14.6. Umbrella 1.0.4, the second source,
runs NATS 2.14.9. Check each exact image and native snapshot API again.
The upstream [2.14.6 memory store](https://github.com/nats-io/nats-server/blob/v2.14.6/server/memstore.go)
does not implement snapshots. A filesystem archive does not capture messages
held in process memory. An earlier test of another image is not current
recovery evidence.

The candidate recovery mode is `drained-memory-cold-restore`. It covers a
fully drained cohort. Recovery of nonempty or in-flight state is unsupported.
This restriction is explicit; it does not claim a general memory export/replay
solution or waive data preservation.

| Stream family | Application state | Required recovery checkpoint |
| --- | --- | --- |
| `rq_<region>_<version>` | Function invocation work | Empty stream and no pending, unacknowledged or redelivered consumer work |
| `requestToFunctionVersion_<region>` | Asynchronous result lookup | No outstanding result lookup records |
| `stateful_session_lookup_<region>` | Active session/reconnect lookup | No active session lookup records |
| `CreateNvcaFunctionTaskStream` | Compute creation commands | Empty stream and no pending consumer work |
| `TerminateNvcaStream` | Compute termination commands | Empty stream and no pending consumer work |

Inventory actual source stream and consumer replication. Preserve each
configuration, including single-replica management streams when present.
Test drained cold restore with the selected source image. Record whether
historical stream sequences and delivery/ack counters reset. Do not report
those counters as preserved without direct evidence.

Before a planned NATS member replacement, also wait for both single-replica
management streams to have no messages, bytes or pending consumer work. Keep
their original replication settings. The member-replacement rehearsal uses
controlled invocation traffic and no new management submissions during that
step. It does not establish tolerance of an unplanned member loss with pending
single-replica management work.

1. Freeze the complete source stream and durable-consumer inventory. Unknown
   stream families or missing consumers require review.
2. Pause callers, finish native tasks and drain invocations and sessions.
   Then pause affected writers. Never purge, cancel or delete work to pass.
3. Capture two complete native observations after the pause. Require zero
   messages/bytes, zero consumer pending/acknowledgement/redelivery counts,
   unchanged configurations and stable last sequences. A failed read is not
   an empty stream. File storage needs a separately reviewed recovery mode.
4. Run `check-recovery` against the frozen baseline and private checkpoint.
   Bind the checkpoint to current contract/lock/baseline hashes, the server
   image digest and pause record. The tool rejects stale or incomplete data.
5. Make a consistent cohort backup, including Kubernetes trust/configuration,
   native stream metadata and the other data stores. Restore into new storage
   while the original identity cohort is stopped. Keep originals and backups.
6. Verify stream names, full configurations, storage, replicas and durable
   names/configurations. Record the expected native sequence/counter reset.
   Resume the same application identities and prove original/new invocation,
   secret and task behavior before accepting the recovery method.

`check-recovery` validates supplied observations and pause declarations. It
cannot prove that writers are stopped and does not execute or qualify restore.
Independent phase records and live full-stack acceptance are still required.
The isolated NATS test alone does not clear that final gate.

Keep nonempty memory streams and active invocations, asynchronous results,
sessions and tasks blocked. Do not convert storage, reset consumers manually,
or replay messages to obtain an empty checkpoint. An operator abandoning the
hop must verify source health before resuming its writers and callers.

## Cold restore startup checks

The restored API can initially return saved readiness status before kubelets reconnect.
Record the restore boot time and check the running services before reading the protected cohort.

1. Require every retained node to become Ready.
   Require each kubelet lease renewal to occur after the restore boot time.
2. Require actual stateful Pod readiness and the expected member placement.
   Treat saved Pod status and ready replica counts as hints, not runtime health evidence.
   Use the service checks below after Pod readiness.
   `kubectl rollout status` does not support OnDelete StatefulSets.
3. Require three Cassandra UN members and a successful authenticated CQL read.
   Gossip membership can recover before the CQL port accepts connections.
   Retain failed transport observations and use a bounded startup deadline.
   Authentication failures or cohort mismatches stop the test.
4. Check every NATS member through its live health and JetStream monitoring endpoints after the restore boot time.
   Require three distinct serving members and the expected JetStream metadata quorum.
   Require the original stream and durable-consumer configurations and the declared drained state.
5. Check every OpenBao member through its live health and leader endpoints after the restore boot time.
   Require initialized, unsealed members in the original cluster, with one active leader and three Raft voters.
   Check the original JWT mounts, JWKS and JWT issuance.
   Do not resume application writers until all NATS and OpenBao member checks pass.
6. Compare the paused data, RF/DC mappings and native NATS configurations before resuming application writers.
7. Require the injector to become Ready.
   Check that a server-side Pod dry run produces the expected injected init container before starting consumers.
8. Check fresh compute kubelet leases and actual original HTTP/gRPC calls after compute recovery.
   After restoring deployment minimums, require actual worker readiness before invoking the original functions.
   An ACTIVE API status alone does not establish a worker connection.
   Retain startup transport failures and use a bounded retry deadline.

Container-based labs must preserve actual network attachments, aliases, addresses and storage ownership.
Docker's saved network mode alone does not identify every active attachment.
Keep the original cohort and every failed attempt available during recovery.

## Component restore boundaries

An isolated OpenBao Raft restore can test original unseal material, cluster
identity, JWT mount accessors, plugin catalog and signing keys. Bind its result
to the snapshot checksum, server image digest and actual restored peers.

That result does not establish a consistent Cassandra, NATS, OpenBao and
Kubernetes recovery point. Full-stack recovery must also preserve source
configuration, Kubernetes trust material, credentials, registrations, workload
identities and cross-service data. Recheck source data validation after restore;
previous Pod UIDs, migration receipts and scan timestamps are stale.
