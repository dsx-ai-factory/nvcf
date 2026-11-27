# Upgrade transition lab

This module audits immutable upgrade inputs and records rehearsal evidence.
It is not a production upgrade runner.

Run from this directory:

```sh
go test -race ./...
go vet ./...
```

Use the standard library. Keep raw command output and credentials in private
run directories. Do not change the default Kubernetes context. Keep source
artifacts unchanged. A successful audit is not live qualification.
