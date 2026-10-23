-- +goose Up
-- Prepared graphs are deliberately outside execution_graphs: the scheduler,
-- claims, and legacy graph routes cannot observe them before an activation
-- contract is implemented in a later migration.
CREATE TABLE prepared_execution_graphs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    graph_json TEXT NOT NULL CHECK (json_valid(graph_json)),
    created_at TIMESTAMP NOT NULL
);

-- +goose Down
DROP TABLE prepared_execution_graphs;
