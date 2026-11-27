# Turn off validation for a supported registry

The NVCF API validates credentials and images for every supported registry. This guide shows how to turn off that validation for one registry. With validation off, you can run container functions without a registry credential in NVCF, or with a credential that the NVCF API cannot check. The example pulls from Amazon ECR with the EKS node IAM role instead of long-lived IAM user keys.

For a registry that NVCF does not support, use [Allowlisting an Unsupported Registry](./registry-allowlist.md) instead. NVCF does not validate unsupported registries.

## What each check does

Each supported registry has two checks in the NVCF API. Both checks are on by default.

| Setting | When the NVCF API runs the check | What the check does |
|---|---|---|
| `credential-validation.enabled` | When you add or update a registry credential | Calls the registry to confirm that the credential works. |
| `artifact-validation.enabled` | When you create or deploy a function | Requires a stored credential for the image hostname, then calls the registry to confirm that the image exists. |

## Decide which check to turn off

Turn off a check only in one of these cases:

- If the cluster has its own access to the registry, turn off `artifact-validation` and store no credential for the registry. You choose how the cluster gets that access. For example, EKS nodes can pull from ECR with the node IAM role.
- If the GPU cluster can reach the registry but the NVCF API cannot, turn off `artifact-validation`. Keep the stored credential, because NVCF still sends it to the cluster for the image pull. To add or update the credential, also turn off `credential-validation`.
- If the NVCF API cannot check a credential that you want to store, turn off `credential-validation`. NVCF then stores the credential without a call to the registry.

Read these effects before you change a setting:

- Each setting applies to every account on the control plane and to every hostname that maps to the registry entry.
- With artifact validation off, function create and deploy no longer check that the image exists. A wrong image path or tag fails at deployment, with `ErrImagePull` on the function pod.
- If NVCF stores no credential for a registry, NVCF sends no image pull secret for it. The cluster must pull the image on its own, through a kubelet credential provider or an image pull secret that the cluster adds to function pods.
- The settings apply to container functions and container tasks. Helm functions and Helm tasks do not support this setup.

## Find the registry entry

Each setting belongs to one registry entry under `nvcf.registries.recognized.container.<key>`. The NVCF API maps the image hostname to an entry:

| Key | Image hostname |
|---|---|
| `ngc` | `nvcr.io` |
| `docker` | `docker.io` |
| `ecr` | `<account-id>.dkr.ecr.<region>.amazonaws.com`, for every AWS account and region |
| `ecr-public` | `public.ecr.aws` |
| `acr` | Any hostname that contains `azurecr.io`, such as `<registry>.azurecr.io` |
| `volcengine` | `<registry>-<region>.cr.volces.com` |
| `harbor` | The `hostname` that you set on the entry |
| `artifactory` | The `hostname` that you set on the entry |

The `harbor` and `artifactory` entries have no default. The NVCF API requires `name` and `hostname` on every entry, so set the checks only on an entry that has both.

## Turn off the checks

1. Add this override to the values of the `api` release. Replace `<key>` with the key from the table. Keep only the check that you want to turn off:

   ```yaml
   api:
     remoteConfig:
       configData:
         nvcf:
           registries:
             recognized:
               container:
                 <key>:
                   artifact-validation:
                     enabled: false
                   credential-validation:
                     enabled: false
   ```

   In a Helmfile deployment, add the override inline to the `api` release, as shown in [Worker Image Version Overrides](./helmfile-installation.md#worker-image-version-overrides). Keep `api.remoteConfig.enabled` set to `true`, which is the chart default.

2. Apply the values with the `helmfile` or `helm upgrade` command that you use for the control plane.

3. Confirm that the NVCF API configuration has the settings:

   ```bash
   kubectl -n nvcf get configmap -l app.kubernetes.io/name=helm-nvcf-api -o yaml \
     | grep -A1 'validation'
   ```

   The output shows `enabled: false` under each check that you turned off.

For container tasks, set `artifact-validation` for the NVCT API under `nvctApi.remoteConfig.configData.nvct.registries.recognized.container.<key>`.

## ECR example with the EKS node IAM role

This example shows one way to pull from ECR without long-lived IAM user keys. The kubelet on each EKS GPU node pulls the image with the node IAM role, and NVCF stores no ECR credential. If you give the cluster access to ECR in another way, replace step 1 with your own setup. Steps 2 to 6 stay the same.

1. On the GPU nodes that run function pods, give the node IAM role read access to your ECR repositories. The AWS managed policy `AmazonEC2ContainerRegistryReadOnly` grants this access. For least privilege, attach the private ECR policy from [Step 2: Create and Attach ECR Policy](./third-party-registries.md#step-2-create-and-attach-ecr-policy) instead. If the repositories are in another AWS account, also add a repository policy in that account that allows the node IAM role to pull images.

2. Delete any ECR credential that NVCF stores. If one exists, NVCF sends it to the cluster, and the pull uses its keys. Find it with `./nvcf-cli registry-credential list`, then delete it as described in [Deleting Registry Credentials](./third-party-registries.md#deleting-registry-credentials).

3. Leave ECR out of `api.accountBootstrap.registryCredentials`. NVCF marks bootstrap credentials as `SYSTEM`, and you cannot delete them later.

4. Turn off artifact validation for the `ecr` entry, as described in [Turn off the checks](#turn-off-the-checks). Credential validation does not apply, because you store no ECR credential:

   ```yaml
   api:
     remoteConfig:
       configData:
         nvcf:
           registries:
             recognized:
               container:
                 ecr:
                   artifact-validation:
                     enabled: false
   ```

5. Create a function with an ECR image. Set the inference and health values for your image:

   ```bash
   ./nvcf-cli function create \
     --name ecr-node-role-test \
     --image "<account-id>.dkr.ecr.<region>.amazonaws.com/<repository>/<image>:<tag>" \
     --inference-url "/health" --inference-port 8080 \
     --health-uri "/health" --health-timeout PT30S
   ```

   The command returns a function ID and a version ID.

6. Deploy the function. Set the GPU and instance type for your cluster. The command uses the function ID and version ID from step 5:

   ```bash
   ./nvcf-cli function deploy create \
     --gpu "<gpu>" \
     --instance-type "<instance-type>"
   ```

   The deployment becomes `ACTIVE` when the GPU node has pulled the image and the function is healthy.

## Troubleshoot validation settings

- If function create returns `Missing CONTAINER registry credential for hostname '<hostname>'`, artifact validation is still on for the registry entry. Repeat step 3 of [Turn off the checks](#turn-off-the-checks). If the ConfigMap is correct, restart the NVCF API:

  ```bash
  kubectl -n nvcf rollout restart deployment -l app.kubernetes.io/name=helm-nvcf-api
  ```

- If the function pod shows `ErrImagePull` or `ImagePullBackOff`, the cluster cannot pull the image. Run `kubectl describe pod` on the function pod to see the registry error. Then check the image path and tag, and the identity or image pull secret that the cluster uses for the registry.
