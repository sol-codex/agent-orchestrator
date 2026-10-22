package store_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func graphTask(id string, state domain.TaskState, resources ...string) domain.ExecutionTask {
	return domain.ExecutionTask{
		ID: id, SliceID: "slice", Objective: "Deliver " + id, Scope: "Scoped work",
		Deliverables: []string{"code"}, AcceptanceCriteria: []string{"reviewed"},
		ValidationProcedures: []string{"test"}, Complexity: "small", State: state,
		Resources: resources,
	}
}

func testGraph(id, project string, tasks ...domain.ExecutionTask) domain.ExecutionGraph {
	return domain.ExecutionGraph{
		SchemaVersion: domain.ExecutionGraphSchemaVersion,
		ID:            id, Revision: 1, ProjectID: domain.ProjectID(project), Objective: "Ship plan",
		Slices: []domain.ExecutionSlice{{ID: "slice", Objective: "Useful slice", Scope: "Code", Deliverables: []string{"code"}, AcceptanceCriteria: []string{"reviewed"}}},
		Tasks:  tasks,
	}
}

func claimFor(t *testing.T, s *sqlite.Store, graphID, taskID, attemptID, provider string, at time.Time, resources ...string) execution.Claim {
	t.Helper()
	graph := mustGraph(t, s, graphID)
	var fingerprint string
	for _, task := range graph.Tasks {
		if task.ID == taskID {
			fingerprint = execution.TaskContractFingerprint(graph, task)
			break
		}
	}
	return execution.Claim{
		GraphID: graphID, ContractFingerprint: fingerprint, TaskID: taskID, AttemptID: attemptID, AgentID: provider + "-worker", Provider: provider,
		Resources: resources, At: at, LeaseExpiresAt: at.Add(time.Minute),
	}
}

func mustGraph(t *testing.T, s *sqlite.Store, id string) domain.ExecutionGraph {
	t.Helper()
	graph, found, err := s.GetExecutionGraph(context.Background(), id)
	if err != nil || !found {
		t.Fatalf("get graph %s: found=%v err=%v", id, found, err)
	}
	return graph
}

func TestExecutionGraphPlanRevisionAndRuntimeFence(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	graph := testGraph("g1", "project", graphTask("t1", domain.TaskPending))
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveExecutionGraph(ctx, graph); !errors.Is(err, execution.ErrGraphConflict) {
		t.Fatalf("stale revision error = %v", err)
	}
	graph.Revision++
	graph.Tasks[0].Scope = "Replanned scope"
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	graph = mustGraph(t, s, "g1")
	if graph.Revision != 2 || graph.Tasks[0].Scope != "Replanned scope" {
		t.Fatalf("replan not preserved: %+v", graph)
	}
	graph.Revision++
	graph.Tasks[0].State = domain.TaskReady
	if err := s.SaveExecutionGraph(ctx, graph); !errors.Is(err, execution.ErrGraphRuntimeFacts) {
		t.Fatalf("plan edit advanced runtime state: %v", err)
	}
	if ok, err := s.MarkReady(ctx, "g1", "t1", time.Now().UTC()); err != nil || !ok {
		t.Fatalf("mark ready: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g1", "t1", "attempt", "codex", time.Now().UTC())); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	graph = mustGraph(t, s, "g1")
	graph.Revision++
	graph.Tasks[0].Scope = "Changed after assignment"
	if err := s.SaveExecutionGraph(ctx, graph); !errors.Is(err, execution.ErrGraphRuntimeFacts) {
		t.Fatalf("active contract edit error = %v", err)
	}
}

func TestExecutionClaimAtomicDependenciesLimitsAndExclusiveResources(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "p1")
	seedProject(t, s, "p2")
	ctx := context.Background()
	now := time.Now().UTC()
	first := graphTask("first", domain.TaskReady, `src\shared.go`)
	dependent := graphTask("dependent", domain.TaskPending)
	dependent.Dependencies = []string{"first"}
	for _, graph := range []domain.ExecutionGraph{
		testGraph("g1", "p1", first, dependent),
		testGraph("g2", "p1", graphTask("other", domain.TaskReady, "./src/shared.go")),
		testGraph("g3", "p2", graphTask("third", domain.TaskReady, "elsewhere.go")),
	} {
		if err := s.SaveExecutionGraph(ctx, graph); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := s.MarkReady(ctx, "g1", "dependent", now); err != nil || ok {
		t.Fatalf("dependent ready before prerequisite: ok=%v err=%v", ok, err)
	}
	claim := claimFor(t, s, "g1", "first", "a1", "claude", now, `src\shared.go`)
	claim.MaxConcurrent = 1
	if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claim); err != nil || ok {
		t.Fatalf("duplicate attempt: ok=%v err=%v", ok, err)
	}
	resourceConflict := claimFor(t, s, "g2", "other", "a2", "gemini", now, "./src/shared.go")
	if ok, err := s.ClaimTask(ctx, resourceConflict); err != nil || ok {
		t.Fatalf("cross-graph resource conflict: ok=%v err=%v", ok, err)
	}
	globalLimit := claimFor(t, s, "g3", "third", "a3", "gemini", now, "elsewhere.go")
	globalLimit.MaxConcurrent = 1
	if ok, err := s.ClaimTask(ctx, globalLimit); err != nil || ok {
		t.Fatalf("global concurrent limit: ok=%v err=%v", ok, err)
	}
	globalLimit.MaxConcurrent = 0
	globalLimit.MaxPerProvider = 1
	globalLimit.Provider = "claude"
	if ok, err := s.ClaimTask(ctx, globalLimit); err != nil || ok {
		t.Fatalf("global provider limit: ok=%v err=%v", ok, err)
	}
	globalLimit.Provider = "gemini"
	if ok, err := s.ClaimTask(ctx, globalLimit); err != nil || !ok {
		t.Fatalf("independent claim: ok=%v err=%v", ok, err)
	}
	graph := mustGraph(t, s, "g1")
	if graph.Revision != 2 || graph.Tasks[0].Assignment == nil || len(graph.Tasks[0].Attempts) != 1 || graph.Tasks[0].Attempts[0].ID != "a1" {
		t.Fatalf("assignment and attempt not durable together: %+v", graph.Tasks[0])
	}
}

func TestExecutionClaimWindowsCaseAlias(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path case folding")
	}
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.SaveExecutionGraph(ctx, testGraph("g1", "project", graphTask("one", domain.TaskReady, "SRC/Feature.ts"))); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveExecutionGraph(ctx, testGraph("g2", "project", graphTask("two", domain.TaskReady, "src/feature.ts"))); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g1", "one", "one", "codex", now, "SRC/Feature.ts")); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g2", "two", "two", "codex", now, "src/feature.ts")); err != nil || ok {
		t.Fatalf("case alias claim: ok=%v err=%v", ok, err)
	}
}

func TestExecutionClaimRejectsStaleTaskContract(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.SaveExecutionGraph(ctx, testGraph("g", "project", graphTask("t", domain.TaskReady))); err != nil {
		t.Fatal(err)
	}
	stale := claimFor(t, s, "g", "t", "stale-attempt", "codex", now)
	graph := mustGraph(t, s, "g")
	graph.Revision++
	graph.Tasks[0].Scope = "Updated worker brief"
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ClaimTask(ctx, stale); err != nil || ok {
		t.Fatalf("stale candidate claim: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g", "t", "fresh-attempt", "codex", now)); err != nil || !ok {
		t.Fatalf("fresh candidate claim: ok=%v err=%v", ok, err)
	}
}

func TestExecutionClaimsIndependentTasksFromOneSnapshot(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	now := time.Now().UTC()
	graph := testGraph("g", "project",
		graphTask("one", domain.TaskReady, "one.go"),
		graphTask("two", domain.TaskReady, "two.go"),
		graphTask("three", domain.TaskReady, "three.go"),
		graphTask("four", domain.TaskReady, "four.go"))
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	var claims []execution.Claim
	for _, task := range graph.Tasks {
		claims = append(claims, claimFor(t, s, "g", task.ID, "attempt-"+task.ID, "codex", now, task.Resources...))
	}
	for _, claim := range claims {
		if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
			t.Fatalf("same-snapshot claim %s: ok=%v err=%v", claim.TaskID, ok, err)
		}
	}
	if got := mustGraph(t, s, "g").Revision; got != 5 {
		t.Fatalf("revision after four claims = %d, want 5", got)
	}
}

func TestExecutionReviewRevisionAttemptLimit(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.SaveExecutionGraph(ctx, testGraph("g", "project", graphTask("t", domain.TaskReady))); err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []string{"first", "second"} {
		claim := claimFor(t, s, "g", "t", attempt, "codex", now)
		if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
			t.Fatalf("claim %s: ok=%v err=%v", attempt, ok, err)
		}
		if ok, err := s.MarkRunning(ctx, "g", "t", attempt, domain.SessionID("session-"+attempt), now); err != nil || !ok {
			t.Fatalf("run %s: ok=%v err=%v", attempt, ok, err)
		}
		if ok, err := s.MarkReview(ctx, "g", "t", attempt, now); err != nil || !ok {
			t.Fatalf("review %s: ok=%v err=%v", attempt, ok, err)
		}
		ok, err := s.ResolveReview(ctx, "g", "t", attempt, execution.ReviewRevise, now)
		if attempt == "first" {
			if err != nil || !ok {
				t.Fatalf("revise first: ok=%v err=%v", ok, err)
			}
		} else if ok || !errors.Is(err, execution.ErrReviewAttemptLimit) {
			t.Fatalf("revise at cap: ok=%v err=%v", ok, err)
		}
	}
	graph := mustGraph(t, s, "g")
	if graph.Tasks[0].State != domain.TaskReview || graph.Tasks[0].Assignment == nil || len(graph.Tasks[0].Attempts) != execution.DefaultMaxAttempts {
		t.Fatalf("attempt cap altered pending review: %+v", graph.Tasks[0])
	}
}

func TestExecutionCancelOnlyUnassignedWork(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	now := time.Now().UTC()
	graph := testGraph("g", "project", graphTask("pending", domain.TaskPending), graphTask("ready", domain.TaskReady))
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.CancelTask(ctx, "g", "pending", now); err != nil || !ok {
		t.Fatalf("cancel pending: ok=%v err=%v", ok, err)
	}
	if ok, err := s.CancelTask(ctx, "g", "pending", now); err != nil || ok {
		t.Fatalf("repeat cancel: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g", "ready", "attempt", "codex", now)); err != nil || !ok {
		t.Fatalf("claim ready: ok=%v err=%v", ok, err)
	}
	if ok, err := s.CancelTask(ctx, "g", "ready", now); err != nil || ok {
		t.Fatalf("cancel active: ok=%v err=%v", ok, err)
	}
	if got := mustGraph(t, s, "g"); got.Revision != 3 || got.Tasks[0].State != domain.TaskCancelled || got.Tasks[1].State != domain.TaskAssigned {
		t.Fatalf("unexpected cancellation facts: %+v", got)
	}
}

func TestExecutionReviewAcceptanceAndRestartRecovery(t *testing.T) {
	dataDir := t.TempDir()
	s, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	seedProject(t, s, "project")
	ctx := context.Background()
	now := time.Now().UTC()
	graph := testGraph("g", "project", graphTask("t", domain.TaskReady, "owned.go"), graphTask("peer", domain.TaskReady, "owned.go"))
	if err := s.SaveExecutionGraph(ctx, graph); err != nil {
		t.Fatal(err)
	}
	claim := claimFor(t, s, "g", "t", "attempt", "codex", now, "owned.go")
	if ok, err := s.ClaimTask(ctx, claim); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// A reconstructed adapter sees the persisted assignment and resource claim.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = sqlite.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g", "peer", "peer-attempt", "gemini", now.Add(2*time.Minute), "owned.go")); err != nil || ok {
		t.Fatalf("expired lease released without outcome: ok=%v err=%v", ok, err)
	}
	if ok, err := s.MarkRunning(ctx, "g", "t", "wrong-attempt", domain.SessionID("session-1"), now); err != nil || ok {
		t.Fatalf("stale running transition: ok=%v err=%v", ok, err)
	}
	if ok, err := s.MarkRunning(ctx, "g", "t", "attempt", domain.SessionID("session-1"), now); err != nil || !ok {
		t.Fatalf("running: ok=%v err=%v", ok, err)
	}
	if ok, err := s.MarkReview(ctx, "g", "t", "attempt", now); err != nil || !ok {
		t.Fatalf("review: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g", "peer", "peer-attempt", "gemini", now.Add(2*time.Minute), "owned.go")); err != nil || ok {
		t.Fatalf("review lost resource ownership: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ResolveReview(ctx, "g", "t", "attempt", execution.ReviewAccept, now); err != nil || !ok {
		t.Fatalf("accept: ok=%v err=%v", ok, err)
	}
	if ok, err := s.ClaimTask(ctx, claimFor(t, s, "g", "peer", "peer-attempt", "gemini", now.Add(2*time.Minute), "owned.go")); err != nil || !ok {
		t.Fatalf("claim released after acceptance: ok=%v err=%v", ok, err)
	}
	graph = mustGraph(t, s, "g")
	if graph.Tasks[0].State != domain.TaskCompleted || graph.Tasks[0].Assignment != nil || graph.Tasks[0].Attempts[0].FinishedAt == nil || graph.Revision != 6 {
		t.Fatalf("review outcome graph: %+v", graph)
	}
}

func TestExecutionConcurrentClaimHasOneWinner(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "project")
	ctx := context.Background()
	if err := s.SaveExecutionGraph(ctx, testGraph("g", "project", graphTask("t", domain.TaskReady, "owned.go"))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var wg sync.WaitGroup
	wins := make(chan bool, 2)
	for _, id := range []string{"one", "two"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ok, err := s.ClaimTask(ctx, claimFor(t, s, "g", "t", id, "codex", now, "owned.go"))
			if err != nil {
				t.Errorf("claim %s: %v", id, err)
			}
			wins <- ok
		}(id)
	}
	wg.Wait()
	close(wins)
	count := 0
	for ok := range wins {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("claim winners = %d, want one", count)
	}
}
