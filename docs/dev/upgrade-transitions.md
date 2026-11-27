# Umbrella upgrade planning reference

This reference covers release inputs and prerequisite contracts for
[issue #3558](https://github.com/dsx-ai-factory/nvcf/issues/3558).
Phase 1 delivers shared planning and WorkloadGate acceptance.
The lab audits artifacts and describes checks. It does not execute an upgrade.

## Version identities and required stop

| Route | Source control / compute / Observability | Target control / compute / Observability |
| --- | --- | --- |
| Legacy NVCF 1.0.1 to umbrella 1.0.4 | 1.0.1 / 1.0.0 / 1.0.0 | 1.0.10 / 1.0.5 / 1.0.0 |
| Umbrella 1.0.4 to umbrella 1.1.1 | 1.0.10 / 1.0.5 / 1.0.0 | 1.1.9 / 1.1.1 / 1.1.0 |

Umbrella 1.0.4 is the required release stop before 1.1.1 for earlier 1.0.x releases.
The 1.1.1 manifest's `previous_version: 1.1.0` records release ancestry.
It does not introduce a 1.1.0 stop.
The legacy 1.0.1 baseline has no umbrella manifest. Use its frozen matrix and original inventories.

The [artifact lock](../../tools/ncp-local-cluster/upgrade-lab/transitions/artifact-lock.json)
pins publication commits, original asset IDs, manifests, bundles and inventory digests.
Read the [first contract](../../tools/ncp-local-cluster/upgrade-lab/transitions/umbrella-1.0.1-to-1.0.4.json),
[second contract](../../tools/ncp-local-cluster/upgrade-lab/transitions/umbrella-1.0.4-to-1.1.1.json)
and [current findings](../../tools/ncp-local-cluster/upgrade-lab/transitions/current-findings.md).
Keep release publication commits separate from each manifest's source provenance commit.

## Published guidance and evidence

The [1.1.1 release notes](https://github.com/dsx-ai-factory/nvcf/blob/2034fdcabd36f9119d4be9ff1ecd4ca9dd3e06b6/deploy/releases/1.1.1/notes.md)
report single-cluster and multi-cluster AWS validation of the published upgrade guides.
Use the [1.0.4 to 1.1.1 procedure](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.0.4-to-1.1.1-upgrade.md)
for its supported starting release and documented limits.
Earlier 1.0.x releases first follow the documented 1.0.4 upgrade stop.

The guide did not live-test control 1.1.9 with compute 1.0.5 during the upgrade window.
Single-member OpenBao is not covered. Cross-cluster AWS compute metrics remain experimental.
Keep these limits visible in plans and procedure links.

Both lab contracts remain `unqualified` with `supported_execution_enabled: false`.
Published procedure evidence does not establish this lab profile's qualification or WorkloadGate acceptance.
The superseded 1.1.0 target and its obsolete evidence are no longer active lab inputs.

## Planning prerequisites

1. Resolve all three endpoint stacks from the pinned released manifests.
   Use the explicit legacy exception only for 1.0.1.
2. Verify original manifest, asset index, bundle and inventory identities.
   Record effective chart/image references and missing coverage.
3. Record source data requirements, migration evidence, expected upstream revisions and runtime health checks.
   Keep known blockers, owners and required evidence visible.
4. Choose the HA mode explicitly and review replica, storage and placement changes.
   The new default is `preferred`.
5. Review Event Ledger, optional callbacks, worker images, registration and private mirror settings.
   Preserve secrets outside plans and reports.
6. Preserve every applicable required stop and the declared stack order.
   A checkpoint inside a transition is not another release stop.

These entries describe checks. Automated discovery, configuration migration and preflight require separate planning.

## Second-hop order

Follow the published procedure: dependencies and migrations, OpenBao checks,
core services, state metrics and autoscaling, applicable Cassandra replication,
then the receipt and control-plane verification before compute clusters.
The source and target retain OpenBao server 2.6.3-nv-1.3.9 and the versioned JWT lifecycle.
Review chart 0.37.1 and migrations 0.23.4 without repeating the first hop's legacy bridge.

Update compute clusters with their original registrations.
Final umbrella 1.1.1 selects Observability 1.1.0.
The guide's optional retention of Observability 1.0.0 is a separately documented profile with limitations.
It does not change the selected umbrella manifest.

## Acceptance boundary

Use the [lab commands](../../tools/ncp-local-cluster/upgrade-lab/README.md)
to fetch and audit immutable inputs. Rebuild reports and their provenance after a lock or contract change.
Old checkpoints cannot establish acceptance for changed identities.

Review planner inputs and dependency contracts under Phase 1.
Keep planner parity and WorkloadGate tests separate from a complete upgrade claim.
Execution, stateful rotation, resume, recovery rehearsals and operator-effort comparisons require separate planning.
