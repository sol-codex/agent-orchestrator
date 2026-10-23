package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
)

func dispatchFenceFixture(t *testing.T) (*Store, *sql.DB, hubAOActivationInput, domain.ExecutionGraph) {
	t.Helper()
	s, db, input, graph := activationStoreFixture(t)
	for _, statement := range []string{
		`CREATE TABLE execution_attempts(attempt_id TEXT PRIMARY KEY,graph_id TEXT NOT NULL REFERENCES execution_graphs(id),task_id TEXT NOT NULL,project_id TEXT NOT NULL REFERENCES projects(id),provider TEXT NOT NULL,state TEXT NOT NULL,started_at TIMESTAMP NOT NULL,finished_at TIMESTAMP)`,
		`CREATE TABLE execution_resource_claims(project_id TEXT NOT NULL REFERENCES projects(id),resource_key TEXT NOT NULL,graph_id TEXT NOT NULL REFERENCES execution_graphs(id),task_id TEXT NOT NULL,attempt_id TEXT NOT NULL REFERENCES execution_attempts(attempt_id),claimed_at TIMESTAMP NOT NULL,lease_expires_at TIMESTAMP NOT NULL,PRIMARY KEY(project_id,resource_key))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	seedHubAOIdentity(t, db)
	if _, created, err := s.commitHubAOActivation(context.Background(), input); err != nil || !created {
		t.Fatalf("commit source fixture: created=%v err=%v", created, err)
	}
	return s, db, input, graph
}

func fenceClaim(t *testing.T, s *Store, graphID, attemptID string) execution.Claim {
	t.Helper()
	graph, found, err := s.GetExecutionGraph(context.Background(), graphID)
	if err != nil || !found {
		t.Fatalf("get graph: found=%v err=%v", found, err)
	}
	now := time.Now().UTC()
	return execution.Claim{GraphID: graphID, TaskID: graph.Tasks[0].ID, AttemptID: attemptID,
		AgentID: "agent", Provider: "codex", ContractFingerprint: execution.TaskContractFingerprint(graph, graph.Tasks[0]),
		Resources: graph.Tasks[0].Resources, At: now, LeaseExpiresAt: now.Add(time.Minute)}
}

func TestHubAODispatchFenceStopsStaleReadyClaimAndSurvivesRestart(t *testing.T) {
	s, db, input, graph := dispatchFenceFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if ok, err := s.MarkReady(ctx, graph.ID, graph.Tasks[0].ID, now); err != nil || !ok {
		t.Fatalf("active Hub graph should become ready: ok=%v err=%v", ok, err)
	}
	claim := fenceClaim(t, s, graph.ID, "fenced-attempt") // Snapshot before revocation.
	if _, err := db.Exec(`UPDATE hub_ao_activations SET fenced_at=? WHERE graph_id=?`, now, graph.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE hub_ao_activations SET fenced_at=NULL WHERE graph_id=?`, graph.ID); err == nil {
		t.Fatal("durable revocation fence was cleared")
	}
	if ok, err := s.ClaimTask(ctx, claim); err != nil || ok {
		t.Fatalf("stale list-to-claim after fence: ok=%v err=%v", ok, err)
	}
	restarted := NewStore(db, db)
	if ok, err := restarted.ClaimTask(ctx, claim); err != nil || ok {
		t.Fatalf("restart claim after fence: ok=%v err=%v", ok, err)
	}
	graphs, err := restarted.ListActiveGraphs(ctx)
	if err != nil || len(graphs) != 1 || graphs[0].ID != graph.ID {
		t.Fatalf("fenced graph hidden from reconciliation: graphs=%v err=%v", graphs, err)
	}
	var attempts, resources int
	if err := db.QueryRow(`SELECT count(*) FROM execution_attempts WHERE graph_id=?`, input.GraphID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM execution_resource_claims WHERE graph_id=?`, input.GraphID).Scan(&resources); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || resources != 0 {
		t.Fatalf("refused claim wrote attempt/resources: %d/%d", attempts, resources)
	}
	got, found, err := restarted.GetExecutionGraph(ctx, graph.ID)
	if err != nil || !found || got.Revision != 2 || got.Tasks[0].State != domain.TaskReady {
		t.Fatalf("refused claim changed ready graph: found=%v err=%v graph=%+v", found, err, got)
	}
	// Ordinary AO graphs have no Hub ledger row and retain normal transitions.
	ordinary := graph
	ordinary.ID = "ordinary-graph"
	if err := restarted.SaveExecutionGraph(ctx, ordinary); err != nil {
		t.Fatal(err)
	}
	if ok, err := restarted.MarkReady(ctx, ordinary.ID, ordinary.Tasks[0].ID, now); err != nil || !ok {
		t.Fatalf("ordinary ready: ok=%v err=%v", ok, err)
	}
	if ok, err := restarted.ClaimTask(ctx, fenceClaim(t, restarted, ordinary.ID, "ordinary-attempt")); err != nil || !ok {
		t.Fatalf("ordinary claim: ok=%v err=%v", ok, err)
	}
}

func TestHubAODispatchFenceRefusesMissingChangedOrRevokedIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change string
	}{
		{"missing", `DELETE FROM hub_ao_control_identity`},
		{"host", `UPDATE hub_ao_control_identity SET host_id='different'`},
		{"store", `UPDATE hub_ao_control_identity SET store_id='different'`},
		{"generation", `UPDATE hub_ao_control_identity SET host_generation=8`},
		{"epoch", `UPDATE hub_ao_control_identity SET authority_epoch=4`},
		{"revoked", `UPDATE hub_ao_activations SET fenced_at=CURRENT_TIMESTAMP`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, _, graph := dispatchFenceFixture(t)
			if _, err := db.Exec(tc.change); err != nil {
				t.Fatal(err)
			}
			if ok, err := s.MarkReady(context.Background(), graph.ID, graph.Tasks[0].ID, time.Now().UTC()); err != nil || ok {
				t.Fatalf("fenced identity became ready: ok=%v err=%v", ok, err)
			}
			got, found, err := s.GetExecutionGraph(context.Background(), graph.ID)
			if err != nil || !found || got.Revision != 1 || got.Tasks[0].State != domain.TaskPending {
				t.Fatalf("refused ready mutated graph: found=%v err=%v graph=%+v", found, err, got)
			}
		})
	}
}

func TestHubAODispatchFenceMissingLedgerFailsClosed(t *testing.T) {
	s, db, _, graph := dispatchFenceFixture(t)
	ctx := context.Background()
	if _, err := db.Exec(`DELETE FROM hub_ao_activations WHERE graph_id=?`, graph.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM hub_ao_graph_origins WHERE graph_id=?`, graph.ID); err == nil {
		t.Fatal("Hub origin marker was deletable")
	}
	if ok, err := s.MarkReady(ctx, graph.ID, graph.Tasks[0].ID, time.Now().UTC()); err != nil || ok {
		t.Fatalf("missing ledger became ready: ok=%v err=%v", ok, err)
	}
	if err := s.SaveExecutionGraph(ctx, graph); !errors.Is(err, execution.ErrGraphConflict) {
		t.Fatalf("ordinary update impersonated a Hub graph with missing ledger: %v", err)
	}
	if _, err := db.Exec(`UPDATE hub_ao_graph_origins SET graph_id='different' WHERE graph_id=?`, graph.ID); err == nil {
		t.Fatal("Hub origin marker was mutable")
	}
}

func TestHubAODispatchFenceMigrationBackfillsOrigin(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/backfill.db?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE execution_graphs(id TEXT PRIMARY KEY)`,
		`CREATE TABLE hub_ao_activations(graph_id TEXT PRIMARY KEY)`,
		`INSERT INTO execution_graphs(id) VALUES('legacy-hub')`,
		`INSERT INTO hub_ao_activations(graph_id) VALUES('legacy-hub')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	migration, err := os.ReadFile("../migrations/20261023201303962_hub_ao_dispatch_fence.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, _, ok := strings.Cut(string(migration), "-- +goose Down")
	if !ok {
		t.Fatal("migration missing Down boundary")
	}
	if _, err := db.Exec(up); err != nil {
		t.Fatalf("migrate existing activation: %v", err)
	}
	var markers int
	if err := db.QueryRow(`SELECT count(*) FROM hub_ao_graph_origins WHERE graph_id='legacy-hub'`).Scan(&markers); err != nil || markers != 1 {
		t.Fatalf("existing Hub graph lost origin marker: count=%d err=%v", markers, err)
	}
}
