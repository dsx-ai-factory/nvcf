ALTER TABLE api_keys_api.keys ADD IF NOT EXISTS nca_id TEXT;
ALTER TABLE api_keys_api.keys_by_owner_and_service ADD IF NOT EXISTS nca_id TEXT;

CREATE TABLE IF NOT EXISTS api_keys_api.keys_by_owner_and_account_and_service
(
    nca_id            TEXT,
    owner_type        TEXT,
    owner_id          TEXT,
    issuer_service_id TEXT,
    key_id            TEXT,
    key_status        TEXT,
    created_at        TIMESTAMP,
    expires_at        TIMESTAMP,
    deletes_at        TIMESTAMP,
    key_details       TEXT,
    PRIMARY KEY ((nca_id, owner_type, owner_id), issuer_service_id, key_id)
);

CREATE TABLE IF NOT EXISTS api_keys_api.lock
(
    name      TEXT,
    lockuntil TIMESTAMP,
    lockedat  TIMESTAMP,
    lockedby  TEXT,
    PRIMARY KEY ((name))
);
