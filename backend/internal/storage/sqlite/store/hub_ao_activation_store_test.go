package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
	_ "modernc.org/sqlite"
)

func activationStoreFixture(t *testing.T) (*Store, *sql.DB, hubAOActivationInput, domain.ExecutionGraph) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/activation.db?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		`CREATE TABLE projects(id TEXT PRIMARY KEY)`,
		`CREATE TABLE execution_graphs(id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),revision INTEGER NOT NULL,graph_json TEXT NOT NULL,created_at TIMESTAMP NOT NULL,updated_at TIMESTAMP NOT NULL)`,
		`CREATE TABLE prepared_execution_graphs(id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),graph_json TEXT NOT NULL,created_at TIMESTAMP NOT NULL,prepare_id TEXT NOT NULL UNIQUE,graph_bytes_sha256 TEXT NOT NULL,receipt_identity_sha256 TEXT NOT NULL)`,
		`INSERT INTO projects(id) VALUES('project')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		"../migrations/20261023195138535_hub_ao_activation_ledger.sql",
		"../migrations/20261023201303962_hub_ao_dispatch_fence.sql",
	} {
		migration, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		up, _, ok := strings.Cut(string(migration), "-- +goose Down")
		if !ok {
			t.Fatalf("migration %s missing Down boundary", path)
		}
		if _, err := db.Exec(up); err != nil {
			t.Fatalf("migration %s: %v", path, err)
		}
	}
	s := NewStore(db, db)
	graph := domain.ExecutionGraph{
		SchemaVersion: 1, ID: "graph-1", Revision: 1, ProjectID: "project", Objective: "Ship plan",
		Slices: []domain.ExecutionSlice{{ID: "slice-1", Objective: "Useful slice", Scope: "Code", Deliverables: []string{"code"}, AcceptanceCriteria: []string{"reviewed"}}},
		Tasks: []domain.ExecutionTask{{ID: "task-1", SliceID: "slice-1", Objective: "Ship code", Scope: "Code",
			Deliverables: []string{"code"}, AcceptanceCriteria: []string{"reviewed"}, ValidationProcedures: []string{"test"},
			Complexity: "small", State: domain.TaskPending, Resources: []string{"src/task.go"}}},
	}
	receipt, created, err := s.PrepareExecutionGraph(context.Background(), "prepare-1", graph)
	if err != nil || !created {
		t.Fatalf("prepare: created=%v err=%v", created, err)
	}
	now := time.Now().UTC()
	format := func(value time.Time) string { return value.Format("2006-01-02T15:04:05.000Z") }
	input := hubAOActivationInput{
		GraphID: graph.ID, ProjectID: string(graph.ProjectID), PrepareID: receipt.PrepareID,
		GraphBytesSHA256: receipt.GraphBytesSHA256, ReceiptIdentitySHA256: receipt.ReceiptIdentitySHA256,
		GraphContractDigest: strings.Repeat("d", 64), HubAOPlanBindingDigest: strings.Repeat("e", 64),
		GrantID: "grant-1", GrantDigest: strings.Repeat("a", 64), RedemptionID: "redemption-1",
		PermitDigest: strings.Repeat("b", 64), PermitUseBy: format(now.Add(time.Minute)),
		ActivationAttemptID: "attempt-1", CheckID: "check-1", CurrentnessDigest: strings.Repeat("c", 64),
		CurrentnessCheckedAt: format(now.Add(-time.Second)), CurrentnessUseBy: format(now.Add(2 * time.Second)),
		HostID: "host-1", StoreID: "store-1", HostGeneration: 7, AuthorityEpoch: 3,
	}
	return s, db, input, graph
}

func seedHubAOIdentity(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO hub_ao_control_identity(singleton,store_id,host_id,host_generation,authority_epoch) VALUES(1,'store-1','host-1',7,3)`); err != nil {
		t.Fatal(err)
	}
}

func TestHubAOActivationStoreRequiresEnrollmentAndCommitsOnce(t *testing.T) {
	s, db, input, graph := activationStoreFixture(t)
	ctx := context.Background()
	if _, created, err := s.commitHubAOActivation(ctx, input); err == nil || created {
		t.Fatal("absent server-owned enrollment activated graph")
	}
	if _, found, err := s.GetExecutionGraph(ctx, graph.ID); err != nil || found {
		t.Fatalf("graph visible after refusal: found=%v err=%v", found, err)
	}
	seedHubAOIdentity(t, db) // Test-only direct row; production has no writer.
	record, created, err := s.commitHubAOActivation(ctx, input)
	if err != nil || !created || record.ActivationID == "" || record.State != "active" || record.GraphRevision != 1 {
		t.Fatalf("commit: record=%#v created=%v err=%v", record, created, err)
	}
	if _, found, err := s.GetPreparedExecutionGraph(ctx, input.PrepareID, false); err != nil || found {
		t.Fatalf("prepared row remained after commit: found=%v err=%v", found, err)
	}
	if got, found, err := s.GetExecutionGraph(ctx, graph.ID); err != nil || !found || got.ID != graph.ID {
		t.Fatalf("active graph missing: found=%v err=%v", found, err)
	}
	if err := s.SaveExecutionGraph(ctx, graph); !errors.Is(err, execution.ErrGraphConflict) {
		t.Fatalf("ordinary graph route changed Hub graph: %v", err)
	}
	// Reconciliation of a lost response returns the same receipt after expiry.
	if _, err := db.Exec(`UPDATE hub_ao_control_identity SET host_generation=8 WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
	expiry, err := hubAOActivationInstant(input.CurrentnessUseBy)
	if err != nil {
		t.Fatal(err)
	}
	if wait := time.Until(expiry) + 20*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	retry, created, err := s.commitHubAOActivation(ctx, input)
	if err != nil || created || retry != record {
		t.Fatalf("identical retry produced another activation: created=%v err=%v", created, err)
	}
	// A new store instance reconstructs the complete persisted receipt after restart.
	restarted := NewStore(db, db)
	reconstructed, found, err := readHubAOActivation(ctx, db, input)
	if err != nil || !found || reconstructed != record ||
		reconstructed.GraphContractDigest != input.GraphContractDigest ||
		reconstructed.HubAOPlanBindingDigest != input.HubAOPlanBindingDigest {
		t.Fatalf("persisted receipt reconstruction: found=%v err=%v", found, err)
	}
	if replay, created, err := restarted.commitHubAOActivation(ctx, input); err != nil || created || replay != record {
		t.Fatalf("restart replay changed receipt: created=%v err=%v", created, err)
	}
	changed := input
	changed.GrantDigest = strings.Repeat("d", 64)
	if _, created, err := s.commitHubAOActivation(ctx, changed); !errors.Is(err, execution.ErrGraphConflict) || created {
		t.Fatalf("changed replay: created=%v err=%v", created, err)
	}
	for _, changed := range []hubAOActivationInput{
		func() hubAOActivationInput { v := input; v.GraphContractDigest = strings.Repeat("9", 64); return v }(),
		func() hubAOActivationInput { v := input; v.HubAOPlanBindingDigest = strings.Repeat("9", 64); return v }(),
	} {
		if _, created, err := restarted.commitHubAOActivation(ctx, changed); !errors.Is(err, execution.ErrGraphConflict) || created {
			t.Fatalf("changed persisted receipt digest replay: created=%v err=%v", created, err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM hub_ao_activations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("activation ledger count=%d err=%v", count, err)
	}
	other := graph
	other.ID = "graph-2"
	receipt, prepared, err := s.PrepareExecutionGraph(ctx, "prepare-2", other)
	if err != nil || !prepared {
		t.Fatalf("prepare second graph: prepared=%v err=%v", prepared, err)
	}
	reused := input
	reused.GraphID, reused.PrepareID = other.ID, receipt.PrepareID
	reused.GraphBytesSHA256, reused.ReceiptIdentitySHA256 = receipt.GraphBytesSHA256, receipt.ReceiptIdentitySHA256
	for _, collision := range []struct {
		name string
		edit func(*hubAOActivationInput)
	}{
		{"grant", func(v *hubAOActivationInput) {
			v.RedemptionID = "redemption-2"
			v.ActivationAttemptID = "attempt-2"
			v.CheckID = "check-2"
		}},
		{"redemption", func(v *hubAOActivationInput) {
			v.GrantID = "grant-2"
			v.ActivationAttemptID = "attempt-2"
			v.CheckID = "check-2"
		}},
		{"attempt", func(v *hubAOActivationInput) {
			v.GrantID = "grant-2"
			v.RedemptionID = "redemption-2"
			v.CheckID = "check-2"
		}},
		{"check", func(v *hubAOActivationInput) {
			v.GrantID = "grant-2"
			v.RedemptionID = "redemption-2"
			v.ActivationAttemptID = "attempt-2"
		}},
	} {
		t.Run(collision.name, func(t *testing.T) {
			candidate := reused
			collision.edit(&candidate)
			if _, created, err := s.commitHubAOActivation(ctx, candidate); !errors.Is(err, execution.ErrGraphConflict) || created {
				t.Fatalf("one-use ID reused on second graph: created=%v err=%v", created, err)
			}
		})
	}
}

func TestHubAOActivationStoreRollsBackAndFencesIdentity(t *testing.T) {
	s, db, input, graph := activationStoreFixture(t)
	seedHubAOIdentity(t, db)
	ctx := context.Background()
	for _, changed := range []hubAOActivationInput{
		func() hubAOActivationInput { v := input; v.HostGeneration++; return v }(),
		func() hubAOActivationInput { v := input; v.AuthorityEpoch++; return v }(),
		func() hubAOActivationInput { v := input; v.StoreID = "other-store"; return v }(),
		func() hubAOActivationInput { v := input; v.ReceiptIdentitySHA256 = strings.Repeat("9", 64); return v }(),
		func() hubAOActivationInput { v := input; v.GraphBytesSHA256 = strings.Repeat("9", 64); return v }(),
		func() hubAOActivationInput { v := input; v.GraphContractDigest = "invalid"; return v }(),
		func() hubAOActivationInput { v := input; v.HubAOPlanBindingDigest = "invalid"; return v }(),
		func() hubAOActivationInput { v := input; v.CurrentnessUseBy = "2000-01-01T00:00:00.000Z"; return v }(),
	} {
		if _, created, err := s.commitHubAOActivation(ctx, changed); err == nil || created {
			t.Fatalf("wrong identity or stale check activated: %#v", changed)
		}
	}
	// Fail after the active-graph INSERT. SQLite must restore the prepared row.
	if _, err := db.Exec(`CREATE TRIGGER refuse_hub_activation BEFORE INSERT ON hub_ao_activations BEGIN SELECT RAISE(ABORT,'test rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, created, err := s.commitHubAOActivation(ctx, input); err == nil || created {
		t.Fatal("forced ledger failure committed active graph")
	}
	if _, found, err := s.GetExecutionGraph(ctx, graph.ID); err != nil || found {
		t.Fatalf("active graph survived rollback: found=%v err=%v", found, err)
	}
	if _, found, err := s.GetPreparedExecutionGraph(ctx, input.PrepareID, false); err != nil || !found {
		t.Fatalf("prepared row lost after rollback: found=%v err=%v", found, err)
	}
	if _, err := db.Exec(`DROP TRIGGER refuse_hub_activation`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	records := make(chan hubAOActivationRecord, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, _, err := s.commitHubAOActivation(ctx, input)
			if err != nil {
				t.Errorf("concurrent retry: %v", err)
			}
			records <- record
		}()
	}
	wg.Wait()
	close(records)
	var first hubAOActivationRecord
	for record := range records {
		if first.ActivationID == "" {
			first = record
		} else if record != first {
			t.Fatalf("concurrent calls returned distinct receipts: %#v %#v", first, record)
		}
	}
}
