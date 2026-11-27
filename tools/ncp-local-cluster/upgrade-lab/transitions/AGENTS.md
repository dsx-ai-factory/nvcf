# Lab upgrade transition contracts

These schema 2 contracts describe unqualified lab candidates.
Resolve umbrella 1.0.4 and 1.1.1 through pinned released manifests.
Version 1.0.1 is a legacy baseline without an umbrella manifest.
Keep released manifests and original inventories immutable.
Bind reports to the current lock and contract digests.
Published guide evidence does not qualify this lab profile or gate integration.

Validate from the repository root:

```sh
go -C tools/ncp-local-cluster/upgrade-lab test -race ./...
go -C tools/ncp-local-cluster/upgrade-lab vet ./...
git diff --check
```

Keep reports public-safe. Store credentials, raw output and backups outside the repository.
