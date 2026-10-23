-- +goose Up
-- No production path writes this fence in this source slice. A later protected
-- control transaction must set it monotonically when Hub revokes authority.
ALTER TABLE hub_ao_activations ADD COLUMN fenced_at TIMESTAMP;
CREATE TRIGGER hub_ao_activation_fence_no_clear BEFORE UPDATE OF fenced_at ON hub_ao_activations
WHEN OLD.fenced_at IS NOT NULL AND (NEW.fenced_at IS NULL OR NEW.fenced_at != OLD.fenced_at)
BEGIN SELECT RAISE(ABORT, 'Hub AO fence is monotonic'); END;
-- Classification survives a missing activation receipt. Existing committed
-- activations are backfilled before the marker becomes immutable.
CREATE TABLE hub_ao_graph_origins (
    graph_id TEXT PRIMARY KEY REFERENCES execution_graphs(id)
);
INSERT INTO hub_ao_graph_origins(graph_id) SELECT graph_id FROM hub_ao_activations;
CREATE TRIGGER hub_ao_graph_origins_no_update BEFORE UPDATE ON hub_ao_graph_origins
BEGIN SELECT RAISE(ABORT, 'Hub AO origin is immutable'); END;
CREATE TRIGGER hub_ao_graph_origins_no_delete BEFORE DELETE ON hub_ao_graph_origins
BEGIN SELECT RAISE(ABORT, 'Hub AO origin is immutable'); END;

-- +goose Down
DROP TRIGGER hub_ao_graph_origins_no_delete;
DROP TRIGGER hub_ao_graph_origins_no_update;
DROP TABLE hub_ao_graph_origins;
DROP TRIGGER hub_ao_activation_fence_no_clear;
ALTER TABLE hub_ao_activations DROP COLUMN fenced_at;
