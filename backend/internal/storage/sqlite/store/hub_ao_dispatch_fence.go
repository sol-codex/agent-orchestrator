package store

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// hubAODispatchAllowed runs inside the same write transaction as READY/claim.
// A graph without an activation ledger row is an ordinary AO graph. An
// activated Hub graph requires an unfenced ledger row and the exact enrolled
// server-owned identity. This does not guard the later external worker Spawn.
func hubAODispatchAllowed(ctx context.Context, db gen.DBTX, graphID string) (bool, error) {
	var hubOrigin, ledgerRows int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM hub_ao_graph_origins WHERE graph_id=?),
		(SELECT COUNT(*) FROM hub_ao_activations WHERE graph_id=?)`, graphID, graphID).Scan(&hubOrigin, &ledgerRows); err != nil {
		return false, err
	}
	if hubOrigin == 0 && ledgerRows == 0 {
		return true, nil
	}
	if hubOrigin != 1 || ledgerRows != 1 {
		return false, nil
	}
	var allowed int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM hub_ao_activations AS a
		JOIN hub_ao_control_identity AS i ON i.singleton=1
			AND i.host_id=a.host_id AND i.store_id=a.store_id
			AND i.host_generation=a.host_generation AND i.authority_epoch=a.authority_epoch
		WHERE a.graph_id=? AND a.state='active' AND a.fenced_at IS NULL`, graphID).Scan(&allowed); err != nil {
		return false, err
	}
	return allowed == 1, nil
}
