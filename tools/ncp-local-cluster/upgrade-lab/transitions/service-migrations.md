# Service migration prerequisites

These are planning requirements for later execution, not completed runtime checks.
Bind actual task availability and completion evidence to exact images and the source data epoch.

## Source lookup requirements

The legacy source SIS 0.7.2 has no InstanceLookupBackfillTask.
Compare each retained instance with its authoritative request before the first hop.
Required deployment and GPU specification IDs must be present and consistent.
Null pairs do not satisfy a required lookup. Conflicts and missing requests block the affected action.

Umbrella 1.0.4 selects SIS 0.7.7, whose task is disabled by default.
Confirm its mode and actual image before any required source repair.
Pause affected writers before the final validation scan and preserve that epoch through backup.
Record scanned, current, skipped, missing-request, mismatch and failure counters.
Target schema Job completion does not replace these source data checks.

## Partial MIGRATE recovery

Stop failed or partial migration runs before deciding whether to repeat them.
Preserve committed null-only repairs and original row identities.
Reconcile every retained instance with its authoritative request.
Conflicting non-null values require a reviewed repair from the migration owner.
Repeat a repair only after proving that its writes remain idempotent for the affected cohort.
Finish with a complete matching validation scan and independent required-ID checks.
A Pod replacement, restore or inconsistent write invalidates earlier scan evidence.

## Target inputs

Umbrella 1.1.1 selects ICMS 0.8.6 and Cassandra migrations 0.19.4.
Review their actual task and SQL payloads, removed consumers and clean-ledger requirements.
Preserve the second hop's versioned OpenBao lifecycle while reviewing migrations 0.23.4.
Review first-hop bridge behavior separately.
Use the [published guide](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.0.4-to-1.1.1-upgrade.md)
and [current findings](current-findings.md) for the exact target and known limitations.
