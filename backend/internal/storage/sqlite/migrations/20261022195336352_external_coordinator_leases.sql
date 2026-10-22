-- +goose Up
CREATE TABLE external_coordinator_leases (
    project_id      TEXT PRIMARY KEY REFERENCES projects (id) ON DELETE CASCADE,
    owner_id        TEXT NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 120),
    display_name    TEXT NOT NULL CHECK (length(display_name) BETWEEN 1 AND 120),
    last_seen_at_ms INTEGER NOT NULL,
    expires_at_ms   INTEGER NOT NULL CHECK (expires_at_ms > last_seen_at_ms)
);

-- The external lease and an AO orchestrator session must not both own a
-- project. The service checks first for a useful 409; these triggers close
-- the race between independently submitted requests.
-- +goose StatementBegin
CREATE TRIGGER external_coordinator_no_ao_session_insert
BEFORE INSERT ON external_coordinator_leases
WHEN EXISTS (
    SELECT 1 FROM sessions
    WHERE project_id = NEW.project_id AND kind = 'orchestrator' AND is_terminated = 0
)
BEGIN
    SELECT RAISE(ABORT, 'AO_ORCHESTRATOR_ACTIVE');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER external_coordinator_no_ao_session_update
BEFORE UPDATE ON external_coordinator_leases
WHEN EXISTS (
    SELECT 1 FROM sessions
    WHERE project_id = NEW.project_id AND kind = 'orchestrator' AND is_terminated = 0
)
BEGIN
    SELECT RAISE(ABORT, 'AO_ORCHESTRATOR_ACTIVE');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER ao_orchestrator_no_external_lease_insert
BEFORE INSERT ON sessions
WHEN NEW.kind = 'orchestrator' AND NEW.is_terminated = 0
    AND EXISTS (
        SELECT 1 FROM external_coordinator_leases
        WHERE project_id = NEW.project_id AND expires_at_ms > unixepoch('now') * 1000
    )
BEGIN
    SELECT RAISE(ABORT, 'EXTERNAL_COORDINATOR_ACTIVE');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER ao_orchestrator_no_external_lease_update
BEFORE UPDATE OF kind, is_terminated ON sessions
WHEN NEW.kind = 'orchestrator' AND NEW.is_terminated = 0
    AND (OLD.kind <> 'orchestrator' OR OLD.is_terminated <> 0)
    AND EXISTS (
        SELECT 1 FROM external_coordinator_leases
        WHERE project_id = NEW.project_id AND expires_at_ms > unixepoch('now') * 1000
    )
BEGIN
    SELECT RAISE(ABORT, 'EXTERNAL_COORDINATOR_ACTIVE');
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER ao_orchestrator_no_external_lease_update;
DROP TRIGGER ao_orchestrator_no_external_lease_insert;
DROP TRIGGER external_coordinator_no_ao_session_update;
DROP TRIGGER external_coordinator_no_ao_session_insert;
DROP TABLE external_coordinator_leases;
