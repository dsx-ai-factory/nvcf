# NVCF 1.1.0

NVCF 1.1.0 combines the following stack releases:

| Stack | Version |
| --- | --- |
| Self-Managed | 1.1.5 |
| Compute Plane | 1.1.0 |
| Observability | 1.1.0 |

## Download and install

Download the attached `nvcf-1.1.0.tar.gz`, `manifest.json`, and
`release-assets.json`. The asset index records SHA256 digests. Use the packaged
umbrella archive rather than GitHub's automatically generated source archives.

Extract into a new persistent directory and retain the sibling directories
under `deploy/stacks/`. Use their existing Helmfile/Make installation entrypoints
with separate environment files, secrets, and explicit Kubernetes contexts.
The recorded toolchain is Helm 3.21.4 and Helmfile 1.1.9.

## Upgrade from NVCF 1.0.1

The upgrade procedure and its validation are still in progress. Publication
of this release does not establish a qualified upgrade path. Completion is
tracked in [the upgrade follow-up](https://github.com/dsx-ai-factory/nvcf/issues/3375).

If you run NVCF 1.0.4, do not upgrade to 1.1.0 yet. The guide below covers
only a planned upgrade from 1.0.1; do not apply its procedure to a 1.0.4
deployment. The product's predecessor field does not qualify that upgrade path.

Review the current
[1.0.1 to 1.1.0 upgrade guide](https://github.com/dsx-ai-factory/nvcf/blob/1388f73aa9db949a4af67a4f653c6a7b81e86c46/docs/overview/release-notes/1.0.1-to-1.1.0-upgrade.md)
before planning an upgrade. It covers hostname-policy and high availability
preparation, the single-member Cassandra step, queue draining, and the staged
control-plane order. Preserve backups and
follow the documented rollback limits; replacing the archive does not reverse
database migrations.

The maintained 1.1.0 documentation edition is activated separately as a
selectable version. Version 1.0.4 remains the default until the upgrade
procedure and its validation are complete. Reviewed documentation corrections
can be published under the same 1.1.0 edition without changing release assets.

## Subsequent security patch

Further CVE remediation and eMudhra/emSign security backports are planned for
a subsequent 1.1.x patch. See [the patch release follow-up](https://github.com/dsx-ai-factory/nvcf/issues/3187)
for scope and delivery status. These fixes are not included by this release's
qualification approval.
