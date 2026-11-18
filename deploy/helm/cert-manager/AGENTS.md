# cert-manager wrapper chart

Expose all five image settings in the wrapper values, including the indirect
ACME solver. Do not choose an image source in this chart: `registry`,
`repository`, `imageRegistry` and `imageNamespace` belong in the stack
`base.yaml` or a values file. The chart is pending-fix until its prefilled
sources move there and the wrapper guards them. Empty tags inherit the
vendored chart appVersion. The catalog index is for CI; Helm consumes the
referenced settings directly.

Run from the repository root:

```sh
helm lint deploy/helm/cert-manager
NVCF_IMAGE_CATALOG_E2E=1 go test -C tools/docs-version-sync -run '^TestChartImageCoverage$' -count=1
```

The coverage test checks every tracked chart, including this one. It renders
the CI values scenario `tools/ci/helm-validate-values/cert-manager.yaml` and
ordinary Helm overrides without a cluster.
Follow root QA guidance before live installation. Preserve external-controller
mode and explicit repository/tag/digest overrides.
