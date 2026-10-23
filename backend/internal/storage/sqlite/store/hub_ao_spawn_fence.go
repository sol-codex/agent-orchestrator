package store

import (
	"context"
	"database/sql"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// CanSpawnExecutionAttempt checks one committed ASSIGNED attempt against a
// single SQLite read snapshot immediately before an external session spawn.
// It cannot make that external spawn atomic with a later revocation commit.
func (s *Store) CanSpawnExecutionAttempt(ctx context.Context, graphID, taskID, attemptID, agentID, provider string) (bool, error) {
	if graphID == "" || taskID == "" || attemptID == "" || agentID == "" || provider == "" {
		return false, nil
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	graph, found, err := readExecutionGraph(ctx, tx, graphID)
	if err != nil || !found {
		return false, err
	}
	i := taskIndex(&graph, taskID)
	if i < 0 || graph.Tasks[i].State != domain.TaskAssigned || graph.Tasks[i].Assignment == nil ||
		graph.Tasks[i].Assignment.AttemptID != attemptID || graph.Tasks[i].Assignment.AgentID != agentID ||
		graph.Tasks[i].Assignment.Provider != provider {
		return false, nil
	}
	var storedGraph, storedTask, storedProject, storedProvider, state string
	err = tx.QueryRowContext(ctx, `SELECT graph_id,task_id,project_id,provider,state FROM execution_attempts WHERE attempt_id=?`, attemptID).
		Scan(&storedGraph, &storedTask, &storedProject, &storedProvider, &state)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	if storedGraph != graphID || storedTask != taskID || storedProject != string(graph.ProjectID) ||
		storedProvider != provider || state != "assigned" {
		return false, nil
	}
	return hubAODispatchAllowed(ctx, tx, graphID)
}
