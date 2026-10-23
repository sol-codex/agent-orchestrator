-- +goose Up
-- Deliberately empty until attended enrollment. No route or startup path
-- provisions this row in this source slice.
CREATE TABLE hub_ao_control_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    store_id TEXT NOT NULL UNIQUE CHECK (length(store_id) > 0),
    host_id TEXT NOT NULL CHECK (length(host_id) > 0),
    host_generation INTEGER NOT NULL CHECK (host_generation > 0),
    authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0)
);

-- A committed row is the one-use activation receipt. Unique source IDs make
-- a changed retry conflict even if it names another prepared graph.
CREATE TABLE hub_ao_activations (
    graph_id TEXT PRIMARY KEY REFERENCES execution_graphs(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    prepare_id TEXT NOT NULL UNIQUE,
    prepared_created_at TIMESTAMP NOT NULL,
    graph_bytes_sha256 TEXT NOT NULL CHECK (length(graph_bytes_sha256) = 64),
    receipt_identity_sha256 TEXT NOT NULL CHECK (length(receipt_identity_sha256) = 64),
    grant_id TEXT NOT NULL UNIQUE,
    grant_digest TEXT NOT NULL CHECK (length(grant_digest) = 64),
    redemption_id TEXT NOT NULL UNIQUE,
    permit_digest TEXT NOT NULL CHECK (length(permit_digest) = 64),
    permit_use_by TEXT NOT NULL,
    activation_attempt_id TEXT NOT NULL UNIQUE,
    check_id TEXT NOT NULL UNIQUE,
    currentness_digest TEXT NOT NULL CHECK (length(currentness_digest) = 64),
    currentness_checked_at TEXT NOT NULL,
    currentness_use_by TEXT NOT NULL,
    host_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    host_generation INTEGER NOT NULL CHECK (host_generation > 0),
    authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
    activation_id TEXT NOT NULL UNIQUE,
    graph_revision INTEGER NOT NULL CHECK (graph_revision = 1),
    state TEXT NOT NULL CHECK (state = 'active'),
    activated_at TIMESTAMP NOT NULL
);

-- +goose Down
DROP TABLE hub_ao_activations;
DROP TABLE hub_ao_control_identity;
