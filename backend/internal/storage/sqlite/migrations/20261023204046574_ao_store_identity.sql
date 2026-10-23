-- +goose Up
-- Empty by default. A later attended enrollment may call EnsureAOStoreID;
-- migration and ordinary daemon startup do not mint an identity.
CREATE TABLE ao_store_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    store_id TEXT NOT NULL UNIQUE CHECK (length(store_id) = 73 AND substr(store_id, 1, 9) = 'ao-store-'),
    created_at TIMESTAMP NOT NULL
);
CREATE TRIGGER ao_store_identity_no_update BEFORE UPDATE ON ao_store_identity
BEGIN SELECT RAISE(ABORT, 'AO store identity is immutable'); END;
CREATE TRIGGER ao_store_identity_no_delete BEFORE DELETE ON ao_store_identity
BEGIN SELECT RAISE(ABORT, 'AO store identity is immutable'); END;

-- +goose Down
DROP TRIGGER ao_store_identity_no_delete;
DROP TRIGGER ao_store_identity_no_update;
DROP TABLE ao_store_identity;
