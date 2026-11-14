# Compatibility Matrix

The table records the stack releases documented in this edition. Umbrella
versions and docs editions identify a combination of independently released
stacks. Stack, chart and image versions do not need matching version numbers.

{/*docs-version-sync:BEGIN compatibility-matrix*/}

## Selected stack releases

| Stack | Selected version | Source tag |
| --- | --- | --- |
| [Self-managed (control plane)](../self-managed/installation.md) | `1.1.5` | `deploy/stacks/self-managed/v1.1.5` |
| [Compute plane](../compute-plane/cluster-management/index.md) | `1.1.0` | `deploy/stacks/nvcf-compute-plane/v1.1.0` |
| [Observability](../observability/observability.md) | `1.1.0` | `deploy/stacks/observability/v1.1.0` |

{/*docs-version-sync:END compatibility-matrix*/}

### Declared compatibility (version catalog)

The version catalog declares which stack releases are compatible with each
other. The declarations for the current edition:

| Stack | Release | Works with |
| --- | --- | --- |
| Self-managed (control plane) | `1.0.10` | Compute plane `1.0.5`, Observability `1.0.0` |
| Compute plane | `1.0.5` | Self-managed `1.0.10`, Observability `1.0.0` |
| Observability | `1.0.0` | Compute plane `1.0.5`, Self-managed `1.0.10` |

Source: the `compatibility` entries in
[`docs/version-catalog/main.yaml`](https://github.com/dsx-ai-factory/nvcf/blob/main/docs/version-catalog/main.yaml).
The generated
block above records the selected releases for this edition; this table records
the declared cross-stack compatibility an operator needs when choosing
releases. Future editions update it with their catalog.

## Compatibility and maintenance policy

Later patch or minor releases can be used when their stack compatibility and
migration requirements are satisfied. "Or later" does not mean any later major
release, same-numbered stack versions, or an arbitrary mix of releases.

- Each stack maintains its latest minor train and the preceding train (N and
  N-1). Older trains receive no further patches.
- Patch upgrades within a train are supported. Cross-stack compatibility
  declarations still govern which stack trains can run together.
- When crossing more than one train, upgrade through the latest patch of each
  intermediate train. Complete its migration and upgrade prerequisites before
  continuing.

A product manifest fixes the stack releases and artifacts shipped in one
umbrella archive. That selection does not replace the stack compatibility
policy with an "only these versions are supported" restriction. A compatible
range does not change the contents of an existing archive.

Follow the [1.0.4 upgrade notes](release-notes/1.0.2-to-1.0.4-upgrade.md)
for the docs edition 1.0.2 baseline and required upgrade order. Use
[Consume an NVCF release](umbrella-releases.md) for package downloads.

For the separate 1.1 upgrade path, see its
[cross-stack upgrade order](release-notes/1.0.1-to-1.1.0-upgrade.md#cross-stack-upgrade-order).
That contract applies to its listed starting releases; it does not change
the exact 1.0.4 selection above.
