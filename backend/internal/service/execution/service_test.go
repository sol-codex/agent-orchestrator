package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type memoryStore struct {
	mu     sync.Mutex
	graphs map[string]domain.ExecutionGraph
	claims []Claim
	lists  int
}

func newMemoryStore(graphs ...domain.ExecutionGraph) *memoryStore {
	s := &memoryStore{graphs: make(map[string]domain.ExecutionGraph)}
	for _, graph := range graphs {
		s.graphs[graph.ID] = graph
	}
	return s
}

func copyGraph(graph domain.ExecutionGraph) domain.ExecutionGraph {
	data, _ := json.Marshal(graph)
	var copied domain.ExecutionGraph
	_ = json.Unmarshal(data, &copied)
	return copied
}

func (s *memoryStore) ListActiveGraphs(context.Context) ([]domain.ExecutionGraph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	graphs := make([]domain.ExecutionGraph, 0, len(s.graphs))
	for _, graph := range s.graphs {
		graphs = append(graphs, copyGraph(graph))
	}
	return graphs, nil
}

func (s *memoryStore) listCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

func (s *memoryStore) task(graphID, taskID string) (*domain.ExecutionGraph, *domain.ExecutionTask) {
	graph, ok := s.graphs[graphID]
	if !ok {
		return nil, nil
	}
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == taskID {
			return &graph, &graph.Tasks[i]
		}
	}
	return nil, nil
}

func (s *memoryStore) update(graph domain.ExecutionGraph) { s.graphs[graph.ID] = graph }

func (s *memoryStore) MarkReady(_ context.Context, graphID, taskID string, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, task := s.task(graphID, taskID)
	if task == nil || (task.State != domain.TaskPending && task.State != domain.TaskFailed) || !dependenciesComplete(*task, taskStates(*graph)) {
		return false, nil
	}
	task.State = domain.TaskReady
	s.update(*graph)
	return true, nil
}

func (s *memoryStore) ClaimTask(_ context.Context, claim Claim) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, task := s.task(claim.GraphID, claim.TaskID)
	if task == nil || task.State != domain.TaskReady || !dependenciesComplete(*task, taskStates(*graph)) {
		return false, nil
	}
	active, sameProvider := 0, 0
	resources := map[string]struct{}{}
	for _, otherGraph := range s.graphs {
		for _, other := range otherGraph.Tasks {
			if other.State != domain.TaskAssigned && other.State != domain.TaskRunning {
				continue
			}
			active++
			if other.Assignment != nil {
				for _, prior := range s.claims {
					if prior.AttemptID == other.Assignment.AttemptID && prior.Provider == claim.Provider {
						sameProvider++
					}
				}
			}
			if otherGraph.ProjectID == graph.ProjectID {
				for _, resource := range other.Resources {
					resources[resource] = struct{}{}
				}
			}
		}
	}
	if claim.MaxConcurrent > 0 && active >= claim.MaxConcurrent || claim.MaxPerProvider > 0 && sameProvider >= claim.MaxPerProvider {
		return false, nil
	}
	for _, resource := range claim.Resources {
		if _, held := resources[resource]; held {
			return false, nil
		}
	}
	task.State = domain.TaskAssigned
	task.Assignment = &domain.TaskAssignment{AgentID: claim.AgentID, Provider: claim.Provider, AttemptID: claim.AttemptID, AssignedAt: claim.At}
	task.Attempts = append(task.Attempts, domain.TaskAttempt{ID: claim.AttemptID, AgentID: claim.AgentID, StartedAt: claim.At})
	s.claims = append(s.claims, claim)
	s.update(*graph)
	return true, nil
}

func (s *memoryStore) MarkRunning(_ context.Context, graphID, taskID, attemptID string, sessionID domain.SessionID, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, task := s.task(graphID, taskID)
	if task == nil || task.State != domain.TaskAssigned || task.Assignment == nil || task.Assignment.AttemptID != attemptID {
		return false, nil
	}
	task.State = domain.TaskRunning
	task.Assignment.SessionID = &sessionID
	task.Attempts[len(task.Attempts)-1].SessionID = &sessionID
	s.update(*graph)
	return true, nil
}

func (s *memoryStore) MarkReview(_ context.Context, graphID, taskID, attemptID string, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, task := s.task(graphID, taskID)
	if task == nil || task.State != domain.TaskRunning || task.Assignment == nil || task.Assignment.AttemptID != attemptID {
		return false, nil
	}
	task.State = domain.TaskReview
	s.update(*graph)
	return true, nil
}

func (s *memoryStore) ResolveReview(_ context.Context, graphID, taskID, attemptID string, decision ReviewDecision, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, task := s.task(graphID, taskID)
	if task == nil || task.State != domain.TaskReview || task.Assignment == nil || task.Assignment.AttemptID != attemptID {
		return false, nil
	}
	switch decision {
	case ReviewAccept:
		task.State = domain.TaskCompleted
	case ReviewRevise:
		task.State = domain.TaskReady
	case ReviewReject:
		task.State = domain.TaskFailed
	default:
		return false, errors.New("invalid review decision")
	}
	task.Attempts[len(task.Attempts)-1].FinishedAt = &at
	task.Attempts[len(task.Attempts)-1].Retryable = false
	task.Assignment = nil
	s.update(*graph)
	return true, nil
}

func (s *memoryStore) FailTask(_ context.Context, graphID, taskID, attemptID, message string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, task := s.task(graphID, taskID)
	if task == nil || task.Assignment == nil || task.Assignment.AttemptID != attemptID {
		return false, nil
	}
	task.State = domain.TaskFailed
	task.Attempts[len(task.Attempts)-1].Error = message
	task.Attempts[len(task.Attempts)-1].FinishedAt = &at
	task.Attempts[len(task.Attempts)-1].Retryable = true
	task.Assignment = nil
	s.update(*graph)
	return true, nil
}

func (s *memoryStore) state(graphID, taskID string) domain.TaskState {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, task := s.task(graphID, taskID)
	if task == nil {
		return ""
	}
	return task.State
}

func (s *memoryStore) claimIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, len(s.claims))
	for i, claim := range s.claims {
		ids[i] = claim.TaskID
	}
	return ids
}

type fakeDispatcher struct {
	mu       sync.Mutex
	spawned  []string
	sessions map[string]domain.SessionID
	observed map[domain.SessionID]WorkerObservation
	blocked  map[string]<-chan struct{}
	fail     map[string]bool
}

func newDispatcher() *fakeDispatcher {
	return &fakeDispatcher{sessions: map[string]domain.SessionID{}, observed: map[domain.SessionID]WorkerObservation{}, blocked: map[string]<-chan struct{}{}, fail: map[string]bool{}}
}

func (d *fakeDispatcher) Spawn(ctx context.Context, _ domain.ExecutionGraph, task domain.ExecutionTask, _ Agent, attemptID string) (domain.SessionID, error) {
	d.mu.Lock()
	d.spawned = append(d.spawned, task.ID)
	block := d.blocked[task.ID]
	fail := d.fail[task.ID]
	d.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if fail {
		return "", errors.New("worker launch failed")
	}
	id := domain.SessionID("session-" + attemptID)
	d.mu.Lock()
	d.sessions[attemptID] = id
	d.observed[id] = WorkerObservation{State: WorkerRunning}
	d.mu.Unlock()
	return id, nil
}

func (d *fakeDispatcher) Lookup(_ context.Context, attemptID string) (domain.SessionID, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id, ok := d.sessions[attemptID]
	return id, ok, nil
}

func (d *fakeDispatcher) Inspect(_ context.Context, id domain.SessionID) (WorkerObservation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.observed[id], nil
}

func (d *fakeDispatcher) observedTask(store *memoryStore, graphID, taskID string, state WorkerState) {
	store.mu.Lock()
	_, task := store.task(graphID, taskID)
	var id domain.SessionID
	if task != nil && task.Assignment != nil && task.Assignment.SessionID != nil {
		id = *task.Assignment.SessionID
	}
	store.mu.Unlock()
	d.mu.Lock()
	d.observed[id] = WorkerObservation{State: state}
	d.mu.Unlock()
}

func (d *fakeDispatcher) spawnCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.spawned)
}

type staticCapacity []Agent

type failedLookupDispatcher struct{ *fakeDispatcher }

func (failedLookupDispatcher) Lookup(context.Context, string) (domain.SessionID, bool, error) {
	return "", false, LaunchFailure{Reason: "session launch failed"}
}

func (c staticCapacity) Available(context.Context, domain.ProjectID) ([]Agent, error) {
	return append([]Agent(nil), c...), nil
}

type staticValidator struct{ decision ReviewDecision }

func (v staticValidator) Validate(context.Context, domain.ExecutionGraph, domain.ExecutionTask) (ReviewDecision, error) {
	return v.decision, nil
}

type gatedValidator struct {
	entered chan struct{}
	release chan struct{}
}

func (v gatedValidator) Validate(ctx context.Context, _ domain.ExecutionGraph, _ domain.ExecutionTask) (ReviewDecision, error) {
	select {
	case v.entered <- struct{}{}:
	default:
	}
	select {
	case <-v.release:
		return ReviewAccept, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func graph(tasks ...domain.ExecutionTask) domain.ExecutionGraph {
	return domain.ExecutionGraph{SchemaVersion: domain.ExecutionGraphSchemaVersion, ID: "g", Revision: 1,
		ProjectID: "p", Objective: "deliver", Slices: []domain.ExecutionSlice{{ID: "s", Objective: "slice", AcceptanceCriteria: []string{"works"}}}, Tasks: tasks}
}

func task(id string, state domain.TaskState, dependencies ...string) domain.ExecutionTask {
	return domain.ExecutionTask{ID: id, SliceID: "s", Objective: id, AcceptanceCriteria: []string{"works"},
		ValidationProcedures: []string{"check"}, Complexity: "small", State: state, Dependencies: dependencies}
}

func agents(n int) staticCapacity {
	result := make(staticCapacity, n)
	for i := range result {
		result[i] = Agent{ID: fmt.Sprintf("agent-%d", i), Provider: "test", Slots: 1}
	}
	return result
}

func newService(t *testing.T, store *memoryStore, dispatcher *fakeDispatcher, capacity staticCapacity, cfg Config) *Service {
	t.Helper()
	var next int
	cfg.NewAttemptID = func() string { next++; return fmt.Sprintf("attempt-%d", next) }
	svc, err := New(store, dispatcher, capacity, staticValidator{decision: ReviewAccept}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for asynchronous execution")
}

func TestTickDispatchesFourIndependentTasksWithoutWaitingForFirstSpawn(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady), task("b", domain.TaskReady), task("c", domain.TaskReady), task("d", domain.TaskReady)))
	dispatcher := newDispatcher()
	blocked := make(chan struct{})
	dispatcher.blocked["a"] = blocked
	svc := newService(t, store, dispatcher, agents(4), Config{})
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return dispatcher.spawnCount() == 4 })
	if len(store.claimIDs()) != 4 {
		t.Fatalf("claims = %v, want four", store.claimIDs())
	}
	close(blocked)
	svc.work.Wait()
}

func TestDefinitiveLaunchFailureReleasesAssignedAttempt(t *testing.T) {
	assigned := task("a", domain.TaskAssigned)
	assigned.Assignment = &domain.TaskAssignment{AgentID: "agent-0", Provider: "test", AttemptID: "attempt-old", AssignedAt: time.Now().Add(-time.Minute)}
	assigned.Attempts = []domain.TaskAttempt{{ID: "attempt-old", AgentID: "agent-0", StartedAt: assigned.Assignment.AssignedAt}}
	store := newMemoryStore(graph(assigned))
	svc, err := New(store, failedLookupDispatcher{newDispatcher()}, agents(1), staticValidator{decision: ReviewAccept}, Config{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskFailed })
	svc.work.Wait()
	store.mu.Lock()
	_, got := store.task("g", "a")
	if got.Assignment != nil || len(got.Attempts) != 1 || got.Attempts[0].Error != "session launch failed" {
		t.Fatalf("failed attempt = %+v", got)
	}
	store.mu.Unlock()
}

func TestAmbiguousSpawnErrorRetainsAssignmentUntilLateSessionAppears(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady)))
	dispatcher := newDispatcher()
	dispatcher.fail["a"] = true
	svc := newService(t, store, dispatcher, agents(1), Config{MaxAttempts: 2})
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	svc.work.Wait()
	if got := store.state("g", "a"); got != domain.TaskAssigned {
		t.Fatalf("ambiguous no-row launch state = %s, want assigned", got)
	}
	// Reconciliation after the lease must not release ownership or retry.
	store.mu.Lock()
	current, active := store.task("g", "a")
	active.Assignment.AssignedAt = time.Now().Add(-time.Hour)
	store.update(*current)
	store.mu.Unlock()
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	svc.work.Wait()
	if got := store.state("g", "a"); got != domain.TaskAssigned || dispatcher.spawnCount() != 1 {
		t.Fatalf("expired unresolved launch: state=%s spawns=%d", got, dispatcher.spawnCount())
	}
	// A late durable session is adopted under the same attempt ID.
	dispatcher.mu.Lock()
	dispatcher.sessions["attempt-1"] = "late-session"
	dispatcher.mu.Unlock()
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskRunning })
	svc.work.Wait()
	if dispatcher.spawnCount() != 1 {
		t.Fatalf("late adoption spawned %d times", dispatcher.spawnCount())
	}
}

func TestTaskContractFingerprintIgnoresRuntimeButFencesPlanEdit(t *testing.T) {
	plan := graph(task("a", domain.TaskReady))
	base := TaskContractFingerprint(plan, plan.Tasks[0])
	claimed := plan.Tasks[0]
	claimed.State = domain.TaskAssigned
	claimed.Assignment = &domain.TaskAssignment{AgentID: "agent-0", AttemptID: "attempt-1"}
	claimed.Attempts = []domain.TaskAttempt{{ID: "attempt-1", AgentID: "agent-0"}}
	if got := TaskContractFingerprint(plan, claimed); got != base {
		t.Fatal("runtime facts changed the contract fingerprint")
	}
	claimed.Objective = "changed work"
	if got := TaskContractFingerprint(plan, claimed); got == base {
		t.Fatal("changed task contract retained stale fingerprint")
	}
}

func TestRollingScheduleUnlocksDependentWhilePeerRuns(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady), task("b", domain.TaskPending, "a"), task("peer", domain.TaskReady)))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(3), Config{})
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return store.state("g", "a") == domain.TaskRunning && store.state("g", "peer") == domain.TaskRunning
	})
	if store.state("g", "b") != domain.TaskPending {
		t.Fatal("dependent ran before prerequisite acceptance")
	}
	dispatcher.observedTask(store, "g", "a", WorkerDone)
	for i := 0; i < 4 && store.state("g", "b") != domain.TaskRunning; i++ {
		if err := svc.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		svc.work.Wait()
	}
	if store.state("g", "b") != domain.TaskRunning || store.state("g", "peer") != domain.TaskRunning {
		t.Fatalf("dependent=%s peer=%s", store.state("g", "b"), store.state("g", "peer"))
	}
}

func TestFailureRetryDoesNotInterruptIndependentTask(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady), task("peer", domain.TaskReady)))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(2), Config{MaxAttempts: 2})
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return store.state("g", "a") == domain.TaskRunning && store.state("g", "peer") == domain.TaskRunning
	})
	dispatcher.observedTask(store, "g", "a", WorkerFailed)
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskFailed })
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskRunning })
	if store.state("g", "peer") != domain.TaskRunning {
		t.Fatal("independent worker was interrupted")
	}
}

func TestResourceAndProviderLimitsFenceClaims(t *testing.T) {
	first, second, third := task("a", domain.TaskReady), task("b", domain.TaskReady), task("c", domain.TaskReady)
	first.Resources, second.Resources = []string{"src/shared.go"}, []string{"src/shared.go"}
	store := newMemoryStore(graph(first, second, third))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(3), Config{MaxConcurrent: 2, MaxPerProvider: map[string]int{"test": 2}})
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	svc.work.Wait()
	if len(store.claimIDs()) != 2 || store.state("g", "b") != domain.TaskReady {
		t.Fatalf("claims=%v conflicting task=%s", store.claimIDs(), store.state("g", "b"))
	}
}

func TestNoEligibleWorkDoesNotInventOrDispatch(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskBlocked), task("b", domain.TaskPending, "a")))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(2), Config{})
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.spawnCount() != 0 || len(store.claimIDs()) != 0 {
		t.Fatal("dispatched without eligible work")
	}
}

func TestClosedEventStreamDoesNotBusyLoopWithoutEligibleWork(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskBlocked)))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(1), Config{ReconcileInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	events := make(chan struct{})
	close(events)
	if err := svc.Run(ctx, events); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want deadline", err)
	}
	if calls := store.listCount(); calls > 30 {
		t.Fatalf("list calls = %d, likely busy loop", calls)
	}
	if dispatcher.spawnCount() != 0 {
		t.Fatal("invented work")
	}
}

func TestCriticalPathBreaksEqualPriorityTie(t *testing.T) {
	store := newMemoryStore(graph(task("z", domain.TaskReady), task("a", domain.TaskReady), task("b", domain.TaskPending, "a"), task("c", domain.TaskPending, "b")))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(2), Config{MaxConcurrent: 1})
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if claims := store.claimIDs(); len(claims) != 1 || claims[0] != "a" {
		t.Fatalf("claims=%v, want critical-path task a", claims)
	}
}

func TestWorkerDoneRequiresIndependentValidation(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady)))
	dispatcher := newDispatcher()
	validator := gatedValidator{entered: make(chan struct{}, 1), release: make(chan struct{})}
	svc, err := New(store, dispatcher, agents(1), validator, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskRunning })
	dispatcher.observedTask(store, "g", "a", WorkerDone)
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskReview })
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-validator.entered:
	case <-time.After(time.Second):
		t.Fatal("validator was not called")
	}
	if got := store.state("g", "a"); got != domain.TaskReview {
		t.Fatalf("before validation result: %s, want review", got)
	}
	close(validator.release)
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskCompleted })
}

func TestRestartAdoptsExistingAttemptInsteadOfSpawningAgain(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady)))
	dispatcher := newDispatcher()
	at := time.Now().UTC()
	claim := Claim{GraphID: "g", TaskID: "a", AttemptID: "stable-attempt", AgentID: "agent-0", Provider: "test", At: at, LeaseExpiresAt: at.Add(time.Minute)}
	if ok, err := store.ClaimTask(context.Background(), claim); err != nil || !ok {
		t.Fatalf("claim: ok=%t err=%v", ok, err)
	}
	dispatcher.mu.Lock()
	dispatcher.sessions[claim.AttemptID] = "existing-session"
	dispatcher.observed["existing-session"] = WorkerObservation{State: WorkerRunning}
	dispatcher.mu.Unlock()
	svc := newService(t, store, dispatcher, agents(1), Config{})
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskRunning })
	if dispatcher.spawnCount() != 0 {
		t.Fatal("restart spawned a duplicate worker")
	}
}

func TestPeriodicReconciliationFindsWorkAfterMissedEvent(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskBlocked)))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, agents(1), Config{ReconcileInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx, nil) }()
	store.mu.Lock()
	graph := store.graphs["g"]
	graph.Tasks[0].State = domain.TaskReady
	store.update(graph)
	store.mu.Unlock()
	eventually(t, func() bool { return dispatcher.spawnCount() == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop")
	}
}

func TestBudgetBlockedAgentIsNotDispatched(t *testing.T) {
	store := newMemoryStore(graph(task("a", domain.TaskReady)))
	dispatcher := newDispatcher()
	svc := newService(t, store, dispatcher, staticCapacity{{ID: "agent-0", Provider: "test", Slots: 1, BudgetBlocked: true}}, Config{})
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.spawnCount() != 0 || store.state("g", "a") != domain.TaskReady {
		t.Fatal("budget-blocked agent was used")
	}
}

func TestRejectedValidationIsTerminalEvenWithRetryBudget(t *testing.T) {
	review := task("a", domain.TaskReview)
	review.Assignment = &domain.TaskAssignment{AgentID: "agent-0", Provider: "test", AttemptID: "prior", AssignedAt: time.Now().UTC()}
	review.Attempts = []domain.TaskAttempt{{ID: "prior", AgentID: "agent-0", StartedAt: time.Now().UTC()}}
	store := newMemoryStore(graph(review))
	dispatcher := newDispatcher()
	svc, err := New(store, dispatcher, agents(1), staticValidator{decision: ReviewReject}, Config{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return store.state("g", "a") == domain.TaskFailed })
	if err := svc.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.state("g", "a"); got != domain.TaskFailed || dispatcher.spawnCount() != 0 {
		t.Fatalf("rejected task=%s spawns=%d, want terminal failure", got, dispatcher.spawnCount())
	}
}
