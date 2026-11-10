# Compatibility Matrix

Use this page to pick stack versions that are qualified to run together.

Releases are listed as exact semantic versions (`X.Y.Z`). A requirement such
as `1.0.0 or later` includes later compatible patch and minor releases. Only
the latest minor release train and the one before it are maintained. Upgrade
to a maintained train before moving further.

{/*docs-version-sync:BEGIN compatibility-matrix*/}

## Current stack releases

| Stack | Latest release | Source tag |
| --- | --- | --- |
| [Self-managed (control plane)](../self-managed/installation.md) | `1.1.5` | `deploy/stacks/self-managed/v1.1.5` |
| [Compute plane](../compute-plane/cluster-management/index.md) | `1.1.0` | `deploy/stacks/nvcf-compute-plane/v1.1.0` |
| [Observability](../observability/observability.md) | `1.1.0` | `deploy/stacks/observability/v1.1.0` |

## Compatible stack versions

| Stack | Release | Works with |
| --- | --- | --- |
| Self-managed (control plane) | `1.1.5` | Compute plane `1.1.0` only, Observability `1.1.0` only |
| Compute plane | `1.1.0` | Self-managed (control plane) `1.1.5` only, Observability `1.1.0` only |
| Observability | `1.1.0` | Self-managed (control plane) `1.1.5` only, Compute plane `1.1.0` only |

{/*docs-version-sync:END compatibility-matrix*/}

The tables show the stack versions documented by the selected release.

For the upgrade path from NVCF 1.0.x, see its
[cross-stack upgrade order](release-notes/1.0.1-to-1.1.0-upgrade.md#cross-stack-upgrade-order).
