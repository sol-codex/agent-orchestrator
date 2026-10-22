-- +goose Up

CREATE TABLE execution_graphs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    graph_json TEXT NOT NULL CHECK (json_valid(graph_json)),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE INDEX idx_execution_graphs_project ON execution_graphs(project_id);

-- Attempt IDs are globally unique, allowing a worker spawn to use its attempt
-- ID as a stable idempotency key across coordinator restarts.
CREATE TABLE execution_attempts (
    attempt_id TEXT PRIMARY KEY,
    graph_id TEXT NOT NULL REFERENCES execution_graphs(id),
    task_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id),
    provider TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('assigned', 'running', 'review', 'finished')),
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP
);
CREATE INDEX idx_execution_attempts_active ON execution_attempts(state, provider);

-- Lease expiry marks a claim for reconciliation; it never releases ownership
-- without an authoritative worker outcome or review disposition.
CREATE TABLE execution_resource_claims (
    project_id TEXT NOT NULL REFERENCES projects(id),
    resource_key TEXT NOT NULL,
    graph_id TEXT NOT NULL REFERENCES execution_graphs(id),
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL REFERENCES execution_attempts(attempt_id),
    claimed_at TIMESTAMP NOT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    PRIMARY KEY (project_id, resource_key)
);
CREATE INDEX idx_execution_resource_claims_attempt ON execution_resource_claims(attempt_id);

-- +goose Down
DROP TABLE execution_resource_claims;
DROP TABLE execution_attempts;
DROP TABLE execution_graphs;
