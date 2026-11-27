# Current release-input findings

These findings cover the two active lab contracts and their original inventory comparisons.
Both lab contracts remain unqualified with supported execution disabled.

## New umbrella target

Umbrella 1.1.1 selects control 1.1.9, compute 1.1.1 and Observability 1.1.0.
Its [released manifest](https://github.com/dsx-ai-factory/nvcf/blob/2034fdcabd36f9119d4be9ff1ecd4ca9dd3e06b6/deploy/releases/1.1.1/manifest.json)
and original inventory bytes define the selection.
The manifest's source provenance commit is separate from the umbrella publication commit.

The second comparison has 58 changed, 6 added, 3 removed, zero backward and 3 unordered entries.
The unordered entries are same-version bundled chart references at different source commits.
They cover two topology consumers and the upgrade receipt chart.
Review their bytes and effective use before drawing compatibility conclusions.
LLM-router multiple image variants and removed migration consumers also require profile review.

## OpenBao, NATS and cert-manager

The source and target inventories select OpenBao server 2.6.3-nv-1.3.9,
NATS server 2.14.9-alpine3.22, reloader 0.25.0 and cert-manager v1.20.5.
The obsolete 1.1.0 target's server downgrade is not part of the active comparison.
OpenBao chart 0.37.1 and migrations 0.23.4 still require lifecycle and dependency review.
The second hop retains the versioned JWT lifecycle and does not repeat the legacy bridge.
The first hop retains its separate legacy bridge prerequisites.

## Prerequisites and limits

- Choose `highAvailability.mode` explicitly. The new default is `preferred`.
- Review Event Ledger, which is enabled by default, and the optional worker callback proxy.
- Retain source SIS data checks and inspect actual target ICMS 0.8.6 requirements.
- Retain registration, credentials and settings. Running workers keep their existing image selections.
- Final umbrella 1.1.1 selects Observability 1.1.0. The guide separately permits retaining 1.0.0 with stated limits.
- Resolve helpers and hook images from effective references, including entries absent from original inventories.

Legacy NVCT selects worker-init and task helpers 2.107.0 and ESS agent 1.1.0.
Their original-reference access remains a separate prerequisite for future execution.
Inventory downloads do not prove image access from compute nodes.

## Qualification evidence

The [release notes](https://github.com/dsx-ai-factory/nvcf/blob/2034fdcabd36f9119d4be9ff1ecd4ca9dd3e06b6/deploy/releases/1.1.1/notes.md)
report AWS validation of the published upgrade guides.
The [pinned guide](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.0.4-to-1.1.1-upgrade.md#known-issues)
did not live-test control 1.1.9 with compute 1.0.5 during the upgrade window.
It excludes single-member OpenBao and limits multi-cluster compute metrics.
Keep that external evidence separate from this lab's artifact audit and WorkloadGate acceptance.
No live upgrade, controller test or recovery run establishes qualification in this input update.
