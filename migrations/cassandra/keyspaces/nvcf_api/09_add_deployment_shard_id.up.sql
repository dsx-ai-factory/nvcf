-- Apply before deploying service versions that read or write deployment shards.
ALTER TABLE nvcf_api.functions_deployment_v2 ADD IF NOT EXISTS shard_id INT;

CREATE CUSTOM INDEX IF NOT EXISTS deployment_v2_by_shard_id_sai_idx
    ON nvcf_api.functions_deployment_v2 (shard_id) USING 'StorageAttachedIndex';
