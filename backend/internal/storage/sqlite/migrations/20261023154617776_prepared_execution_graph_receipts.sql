-- +goose Up
ALTER TABLE prepared_execution_graphs ADD COLUMN prepare_id TEXT NOT NULL DEFAULT '';
ALTER TABLE prepared_execution_graphs ADD COLUMN graph_bytes_sha256 TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX prepared_execution_graphs_prepare_id ON prepared_execution_graphs(prepare_id);

-- +goose Down
DROP INDEX prepared_execution_graphs_prepare_id;
ALTER TABLE prepared_execution_graphs DROP COLUMN graph_bytes_sha256;
ALTER TABLE prepared_execution_graphs DROP COLUMN prepare_id;
