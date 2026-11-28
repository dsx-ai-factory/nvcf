# Upgrading API Keys

This guide covers the API Keys upgrade that adds an account ID to every key and
switches new keys to shorter service IDs. Read the whole guide before you
start. Parts of this upgrade cannot be undone.

<Warning>
This upgrade is one way. After the new API Keys pods create their first key,
you cannot return to the previous version. Back up Cassandra before you start,
and do not start the upgrade until you have read every warning on this page.
</Warning>

## What changes

- Every key is stored with the ID of the account it belongs to. Existing keys
  are assigned the account ID configured for API Keys, once, during the
  upgrade.
- New keys use the service IDs `cloud-functions`, `cloud-tasks` and
  `event-ledger`. Keys created before the upgrade keep the old IDs
  (`nvidia-cloud-functions-ncp-service-id-aketm`,
  `nvidia-cloud-tasks-ncp-service-id-nvcttasks` and
  `nvidia-event-ledger-ncp-service-id-ckozoh6f`) and keep working. Requests
  that use either form are accepted.

If you run services that call the introspection or authorization endpoints and
compare the issuer service ID or a policy audience to a configured value, they
see the new IDs for keys created after the upgrade. Update them to accept both
forms before you upgrade.

## Before you upgrade

### Check the account ID

<Warning>
Every existing key is permanently assigned the account ID that API Keys has
configured when the upgrade runs. Changing the setting later does not move
those keys. Confirm the value now, before you upgrade. You cannot correct it
afterwards.
</Warning>

API Keys reads the account ID from the `NVCF_NCA_ID` environment variable. If
you did not set it, the default is `nvcf-default`. Check what the running
deployment uses:

```sh
kubectl -n api-keys get deploy api-keys \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="NVCF_NCA_ID")].value}'
```

An empty result means the default `nvcf-default` is in use. If you need a
different value, set it in your environment file before you upgrade:

```yaml
apikeys:
  env:
    NVCF_NCA_ID: "<your-account-id>"
```

### Back up Cassandra

Take a Cassandra backup and a volume snapshot as your storage provider
describes. The backup section of the
[NVCF 1.0.1 to 1.1.0 upgrade guide](../overview/release-notes/1.0.1-to-1.1.0-upgrade.md)
shows the commands for a self-managed cluster. Keep the backup until you have
verified the upgrade and no longer need the option to restore.

## Upgrade

Upgrade the whole control-plane stack in the usual order. Do not upgrade only
the API Keys release.

<Warning>
The whole stack upgrade applies the database and service registration changes
before the API Keys pods start. If you upgrade only API Keys, the key copy
fails and new keys are rejected with `audience_service_id invalid`.
</Warning>

```sh
cd path/to/nvcf-self-managed-stack
HELMFILE_ENV=<env-name> helmfile sync
```

### Make sure every old pod is replaced

During the rollout, a request can reach an old API Keys pod that does not
recognize keys created by a new one. That request fails, and repeating it
succeeds once it reaches a new pod. The window is usually a few minutes. If you
cannot accept it, change the API Keys deployment strategy to `Recreate` before
you upgrade. All old pods then stop before the new ones start, at the cost of
a short outage.

Do not create, update or delete keys until the rollout is complete and no old
pod is left:

```sh
kubectl -n api-keys rollout status deploy/api-keys
kubectl -n api-keys get pods \
  -o custom-columns=NAME:.metadata.name,IMAGE:.spec.containers[0].image
```

Every API Keys pod must show the new image. Keys created through a pod left
over from the old version can be missing from key lists.

## Wait for the key copy to finish

After the new pods start, API Keys migrates your existing keys. By default
this begins 30 minutes after startup, and it records when it is complete.
Check for the completion record:

```sh
CASS_PW=$(helm get values cassandra -n cassandra-system -a -o json \
  | jq -r .cassandra.dbUser.password)
kubectl -n cassandra-system exec cassandra-0 -c cassandra -- \
  /opt/cassandra/bin/cqlsh -u cassandra -p "$CASS_PW" localhost -e \
  "SELECT record_key FROM api_keys_api.row_update_lock WHERE table_name = 'keys_by_account_index_copy' AND record_key = 'completed';"
```

The upgrade is complete when the output ends with `(1 rows)`. If it shows
`(0 rows)`, wait and check again.

Then confirm the account ID that was assigned:

```sh
kubectl -n cassandra-system exec cassandra-0 -c cassandra -- \
  /opt/cassandra/bin/cqlsh -u cassandra -p "$CASS_PW" localhost -e \
  "SELECT DISTINCT nca_id, owner_type, owner_id FROM api_keys_api.keys_by_owner_and_account_and_service;"
```

Every row must show the account ID you expect.

<Warning>
Do not delete the completion record once you have upgraded to a later release
of API Keys. Nothing in later releases can write it again, so the new key
management endpoints stay unavailable. Do not upgrade to the next API Keys
release until the check above shows `(1 rows)`.
</Warning>

### Run the key migration again

The migration keeps checking for new keys on a regular schedule after it
completes. To switch key lists and lookups back to the data from before the
upgrade and have the migration verify its work again, delete the completion
record:

```sh
kubectl -n cassandra-system exec cassandra-0 -c cassandra -- \
  /opt/cassandra/bin/cqlsh -u cassandra -p "$CASS_PW" localhost -e \
  "DELETE FROM api_keys_api.row_update_lock WHERE table_name = 'keys_by_account_index_copy' AND record_key = 'completed';"
```

Key lists and lookups go back to the data from before the upgrade until the
migration finishes again. Check for the completion record as shown above and
wait for `(1 rows)`.

<Warning>
Delete the completion record only while every API Keys pod runs this release.
Never delete it after you upgrade to a later release of API Keys. Nothing in a
later release can write it again, and the new key management endpoints stay
unavailable. There is no supported way to restore it.
</Warning>

## After the upgrade

If a create, update or delete request returns 503 without a Retry-After
header, the result is unknown. The request may or may not have been applied.

- Create: list your keys before you try again. Repeating a create can produce
  a second key.
- Delete: repeat the delete until it succeeds. Deleting a key that is already
  gone is harmless.

<Warning>
Rolling back to the previous version is not supported once the new pods have
created keys. Keep your backup until you are sure you will not need it.
</Warning>
