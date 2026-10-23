package store

import (
	"context"
	"testing"
	"time"
)

func TestHubAOSpawnFenceChecksAssignedAttemptAndCurrentIdentity(t *testing.T) {
	s, db, _, graph := dispatchFenceFixture(t)
	ctx := context.Background()
	if ok, err := s.MarkReady(ctx, graph.ID, graph.Tasks[0].ID, time.Now().UTC()); err != nil || !ok {
		t.Fatalf("ready: ok=%v err=%v", ok, err)
	}
	claim := fenceClaim(t, s, graph.ID, "hub-attempt")
	if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	check := func(attemptID, agentID, provider string) bool {
		t.Helper()
		allowed, err := s.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, attemptID, agentID, provider)
		if err != nil {
			t.Fatal(err)
		}
		return allowed
	}
	if !check(claim.AttemptID, claim.AgentID, claim.Provider) {
		t.Fatal("committed unfenced Hub attempt refused")
	}
	for _, changed := range []struct{ attempt, agent, provider string }{
		{"other-attempt", claim.AgentID, claim.Provider},
		{claim.AttemptID, "other-agent", claim.Provider},
		{claim.AttemptID, claim.AgentID, "other-provider"},
	} {
		if check(changed.attempt, changed.agent, changed.provider) {
			t.Fatalf("changed assignment passed pre-spawn check: %+v", changed)
		}
	}
	if _, err := db.Exec(`UPDATE hub_ao_activations SET fenced_at=? WHERE graph_id=?`, time.Now().UTC(), graph.ID); err != nil {
		t.Fatal(err)
	}
	if check(claim.AttemptID, claim.AgentID, claim.Provider) {
		t.Fatal("revoked Hub attempt passed pre-spawn check")
	}
	restarted := NewStore(db, db)
	if allowed, err := restarted.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, claim.AttemptID, claim.AgentID, claim.Provider); err != nil || allowed {
		t.Fatalf("restarted store accepted fenced attempt: allowed=%v err=%v", allowed, err)
	}
	got, found, err := s.GetExecutionGraph(ctx, graph.ID)
	if err != nil || !found || got.Tasks[0].Assignment == nil || got.Tasks[0].Assignment.AttemptID != claim.AttemptID {
		t.Fatalf("fence released assigned attempt: found=%v err=%v graph=%+v", found, err, got)
	}
}

func TestHubAOSpawnFenceRefusesMissingLedgerAndChangedEnrollment(t *testing.T) {
	for _, tc := range []struct {
		name, statement string
	}{
		{"missing ledger", `DELETE FROM hub_ao_activations`},
		{"changed host", `UPDATE hub_ao_control_identity SET host_id='different'`},
		{"changed store", `UPDATE hub_ao_control_identity SET store_id='different'`},
		{"changed generation", `UPDATE hub_ao_control_identity SET host_generation=8`},
		{"changed epoch", `UPDATE hub_ao_control_identity SET authority_epoch=4`},
		{"absent enrollment", `DELETE FROM hub_ao_control_identity`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, db, _, graph := dispatchFenceFixture(t)
			ctx := context.Background()
			if ok, err := s.MarkReady(ctx, graph.ID, graph.Tasks[0].ID, time.Now().UTC()); err != nil || !ok {
				t.Fatalf("ready: ok=%v err=%v", ok, err)
			}
			claim := fenceClaim(t, s, graph.ID, "hub-attempt")
			if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
				t.Fatalf("claim: ok=%v err=%v", ok, err)
			}
			if _, err := db.Exec(tc.statement); err != nil {
				t.Fatal(err)
			}
			if allowed, err := s.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, claim.AttemptID, claim.AgentID, claim.Provider); err != nil || allowed {
				t.Fatalf("stale Hub spawn allowed=%v err=%v", allowed, err)
			}
		})
	}
}

func TestOrdinaryAOSpawnFenceRequiresExactCommittedAttempt(t *testing.T) {
	s, db, _, graph := dispatchFenceFixture(t)
	ctx := context.Background()
	graph.ID = "ordinary-graph"
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.MarkReady(ctx, graph.ID, graph.Tasks[0].ID, time.Now().UTC()); err != nil || !ok {
		t.Fatalf("ordinary ready: ok=%v err=%v", ok, err)
	}
	claim := fenceClaim(t, s, graph.ID, "ordinary-attempt")
	if allowed, err := s.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, claim.AttemptID, claim.AgentID, claim.Provider); err != nil || allowed {
		t.Fatalf("uncommitted ordinary attempt allowed=%v err=%v", allowed, err)
	}
	if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
		t.Fatalf("ordinary claim: ok=%v err=%v", ok, err)
	}
	if allowed, err := s.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, claim.AttemptID, claim.AgentID, claim.Provider); err != nil || !allowed {
		t.Fatalf("committed ordinary attempt allowed=%v err=%v", allowed, err)
	}
	if _, err := db.Exec(`UPDATE execution_attempts SET state='running' WHERE attempt_id=?`, claim.AttemptID); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.CanSpawnExecutionAttempt(ctx, graph.ID, graph.Tasks[0].ID, claim.AttemptID, claim.AgentID, claim.Provider); err != nil || allowed {
		t.Fatalf("changed persisted attempt state allowed=%v err=%v", allowed, err)
	}
}
