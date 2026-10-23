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
)

func revocationFixture(t *testing.T) (*Store, *sql.DB, hubAORevocationInput, domain.ExecutionGraph) {
	t.Helper()
	s, db, activation, graph := dispatchFenceFixture(t)
	migration, err := os.ReadFile("../migrations/20261023204833785_hub_ao_revocation_events.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, _, ok := strings.Cut(string(migration), "-- +goose Down")
	if !ok {
		t.Fatal("revocation migration missing Down boundary")
	}
	if _, err := db.Exec(up); err != nil {
		t.Fatal(err)
	}
	var activationID string
	if err := db.QueryRow(`SELECT activation_id FROM hub_ao_activations WHERE graph_id=?`, graph.ID).Scan(&activationID); err != nil {
		t.Fatal(err)
	}
	return s, db, hubAORevocationInput{
		EventID: "event-1", EventDigest: strings.Repeat("f", 64), GraphID: graph.ID,
		ActivationID: activationID, HostID: activation.HostID, StoreID: activation.StoreID,
		HostGeneration: activation.HostGeneration, AuthorityEpoch: activation.AuthorityEpoch,
	}, graph
}

func TestHubAORevocationStoreFencesAssignedAndReplaysAfterRestart(t *testing.T) {
	s, db, input, graph := revocationFixture(t)
	ctx := context.Background()
	if ok, err := s.MarkReady(ctx, graph.ID, graph.Tasks[0].ID, time.Now().UTC()); err != nil || !ok {
		t.Fatalf("ready: ok=%v err=%v", ok, err)
	}
	claim := fenceClaim(t, s, graph.ID, "assigned-before-fence")
	if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	record, created, err := s.commitHubAORevocation(ctx, input)
	if err != nil || !created || record.FencedAt.IsZero() {
		t.Fatalf("commit: record=%+v created=%v err=%v", record, created, err)
	}
	for _, table := range []string{"execution_attempts", "execution_resource_claims"} {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM `+table+` WHERE graph_id=?`, graph.ID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s after fence: count=%d err=%v", table, count, err)
		}
	}
	got, found, err := s.GetExecutionGraph(ctx, graph.ID)
	if err != nil || !found || got.Tasks[0].Assignment == nil || got.Tasks[0].Assignment.AttemptID != claim.AttemptID {
		t.Fatalf("fence changed ASSIGNED graph: found=%v err=%v graph=%+v", found, err, got)
	}
	restarted := NewStore(db, db)
	if allowed, err := restarted.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, claim.AttemptID, claim.AgentID, claim.Provider); err != nil || allowed {
		t.Fatalf("restarted store accepted fenced attempt: allowed=%v err=%v", allowed, err)
	}
	if replay, created, err := restarted.commitHubAORevocation(ctx, input); err != nil || created || !replay.FencedAt.Equal(record.FencedAt) {
		t.Fatalf("lost-response replay: record=%+v created=%v err=%v", replay, created, err)
	}
	changed := input
	changed.EventDigest = strings.Repeat("e", 64)
	if _, created, err := restarted.commitHubAORevocation(ctx, changed); !errors.Is(err, execution.ErrGraphConflict) || created {
		t.Fatalf("changed event replay: created=%v err=%v", created, err)
	}
	changed = input
	changed.EventID = "event-2"
	if _, created, err := restarted.commitHubAORevocation(ctx, changed); !errors.Is(err, execution.ErrGraphConflict) || created {
		t.Fatalf("second event for graph: created=%v err=%v", created, err)
	}
	if _, err := db.Exec(`DELETE FROM hub_ao_revocation_events WHERE event_id=?`, input.EventID); err == nil {
		t.Fatal("revocation event was deletable")
	}
	if _, err := db.Exec(`UPDATE hub_ao_revocation_events SET event_digest=? WHERE event_id=?`, strings.Repeat("a", 64), input.EventID); err == nil {
		t.Fatal("revocation event was mutable")
	}
}

func TestHubAORevocationStoreRefusesChangedAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		mutate    func(*hubAORevocationInput)
	}{
		{name: "missing control", sql: `DELETE FROM hub_ao_control_identity`},
		{name: "new generation", sql: `UPDATE hub_ao_control_identity SET host_generation=8`},
		{name: "new epoch", sql: `UPDATE hub_ao_control_identity SET authority_epoch=4`},
		{name: "other host", sql: `UPDATE hub_ao_control_identity SET host_id='other'`},
		{name: "other store", sql: `UPDATE hub_ao_control_identity SET store_id='other'`},
		{name: "wrong activation", mutate: func(in *hubAORevocationInput) { in.ActivationID = "other" }},
		{name: "bad digest", mutate: func(in *hubAORevocationInput) { in.EventDigest = "bad" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, input, graph := revocationFixture(t)
			if tc.sql != "" {
				if _, err := db.Exec(tc.sql); err != nil {
					t.Fatal(err)
				}
			}
			if tc.mutate != nil {
				tc.mutate(&input)
			}
			if _, created, err := s.commitHubAORevocation(context.Background(), input); err == nil || created {
				t.Fatalf("changed authority committed: created=%v err=%v", created, err)
			}
			var fenced sql.NullTime
			if err := db.QueryRow(`SELECT fenced_at FROM hub_ao_activations WHERE graph_id=?`, graph.ID).Scan(&fenced); err != nil || fenced.Valid {
				t.Fatalf("refusal fenced graph: fence=%v err=%v", fenced, err)
			}
		})
	}
}

func TestHubAORevocationStoreRollsBackAndSerializes(t *testing.T) {
	s, db, input, graph := revocationFixture(t)
	ctx := context.Background()
	if _, err := db.Exec(`CREATE TRIGGER refuse_revocation BEFORE INSERT ON hub_ao_revocation_events BEGIN SELECT RAISE(ABORT,'test rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, created, err := s.commitHubAORevocation(ctx, input); err == nil || created {
		t.Fatalf("insert failure succeeded: created=%v err=%v", created, err)
	}
	var fenced sql.NullTime
	if err := db.QueryRow(`SELECT fenced_at FROM hub_ao_activations WHERE graph_id=?`, graph.ID).Scan(&fenced); err != nil || fenced.Valid {
		t.Fatalf("failed insert left fence: %v %v", fenced, err)
	}
	if _, err := db.Exec(`DROP TRIGGER refuse_revocation`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	type result struct {
		created bool
		err     error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, created, err := s.commitHubAORevocation(ctx, input)
			results <- result{created, err}
		}()
	}
	wg.Wait()
	close(results)
	createdCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent commits created %d events", createdCount)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM hub_ao_revocation_events WHERE graph_id=?`, graph.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("event count=%d err=%v", count, err)
	}
}
