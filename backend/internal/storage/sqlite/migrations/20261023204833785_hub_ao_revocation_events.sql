-- +goose Up
-- Dark ledger: no route or daemon caller writes a revocation in this slice.
CREATE TABLE hub_ao_revocation_events (
    event_id TEXT PRIMARY KEY,
    event_digest TEXT NOT NULL CHECK (length(event_digest) = 64),
    graph_id TEXT NOT NULL UNIQUE REFERENCES hub_ao_activations(graph_id),
    activation_id TEXT NOT NULL UNIQUE,
    host_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    host_generation INTEGER NOT NULL CHECK (host_generation > 0),
    authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
    fenced_at TIMESTAMP NOT NULL
);
CREATE TRIGGER hub_ao_revocation_events_no_update BEFORE UPDATE ON hub_ao_revocation_events
BEGIN SELECT RAISE(ABORT, 'Hub AO revocation event is immutable'); END;
CREATE TRIGGER hub_ao_revocation_events_no_delete BEFORE DELETE ON hub_ao_revocation_events
BEGIN SELECT RAISE(ABORT, 'Hub AO revocation event is immutable'); END;

-- +goose Down
DROP TRIGGER hub_ao_revocation_events_no_delete;
DROP TRIGGER hub_ao_revocation_events_no_update;
DROP TABLE hub_ao_revocation_events;
