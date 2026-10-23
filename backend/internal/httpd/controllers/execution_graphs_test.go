package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
)

type fakeExecutionGraphStore struct {
	graphs      map[string]domain.ExecutionGraph
	prepared    map[string]domain.ExecutionGraph
	saveErr     error
	getErr      error
	listErr     error
	reviewErr   error
	readyErr    error
	cancelErr   error
	saves       int
	reviewCalls int
	readyCalls  int
	cancelCalls int
	reviewed    execution.ReviewDecision
	evidence    domain.TaskReviewRecord
}

func (f *fakeExecutionGraphStore) PrepareExecutionGraph(_ context.Context, graph domain.ExecutionGraph) (bool, error) {
	if f.saveErr != nil {
		return false, f.saveErr
	}
	if _, active := f.graphs[graph.ID]; active {
		return false, execution.ErrGraphConflict
	}
	if previous, exists := f.prepared[graph.ID]; exists {
		if !reflect.DeepEqual(previous, graph) {
			return false, execution.ErrGraphConflict
		}
		return false, nil
	}
	if f.prepared == nil {
		f.prepared = make(map[string]domain.ExecutionGraph)
	}
	f.prepared[graph.ID] = graph
	return true, nil
}

func (f *fakeExecutionGraphStore) SaveExecutionGraph(_ context.Context, graph domain.ExecutionGraph) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saves++
	if f.graphs == nil {
		f.graphs = make(map[string]domain.ExecutionGraph)
	}
	f.graphs[graph.ID] = graph
	return nil
}

func (f *fakeExecutionGraphStore) GetExecutionGraph(_ context.Context, id string) (domain.ExecutionGraph, bool, error) {
	if f.getErr != nil {
		return domain.ExecutionGraph{}, false, f.getErr
	}
	graph, found := f.graphs[id]
	return graph, found, nil
}

func (f *fakeExecutionGraphStore) ListActiveGraphs(context.Context) ([]domain.ExecutionGraph, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	graphs := make([]domain.ExecutionGraph, 0, len(f.graphs))
	for _, graph := range f.graphs {
		graphs = append(graphs, graph)
	}
	return graphs, nil
}

func (f *fakeExecutionGraphStore) ResolveReview(_ context.Context, graphID, taskID, attemptID string, decision execution.ReviewDecision, _ time.Time) (bool, error) {
	return f.ResolveReviewWithEvidence(context.Background(), graphID, taskID, attemptID, decision, domain.TaskReviewRecord{}, time.Time{})
}

func (f *fakeExecutionGraphStore) ResolveReviewWithEvidence(_ context.Context, graphID, taskID, attemptID string, decision execution.ReviewDecision, evidence domain.TaskReviewRecord, _ time.Time) (bool, error) {
	f.reviewCalls++
	f.reviewed = decision
	f.evidence = evidence
	if f.reviewErr != nil {
		return false, f.reviewErr
	}
	graph, found := f.graphs[graphID]
	if !found {
		return false, nil
	}
	graph.Tasks = append([]domain.ExecutionTask(nil), graph.Tasks...)
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == taskID && graph.Tasks[i].State == domain.TaskReview &&
			graph.Tasks[i].Assignment != nil && graph.Tasks[i].Assignment.AttemptID == attemptID {
			switch decision {
			case execution.ReviewAccept:
				graph.Tasks[i].State = domain.TaskCompleted
			case execution.ReviewRevise:
				graph.Tasks[i].State = domain.TaskReady
			case execution.ReviewReject:
				graph.Tasks[i].State = domain.TaskFailed
			}
			graph.Revision++
			f.graphs[graphID] = graph
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeExecutionGraphStore) MarkReady(_ context.Context, graphID, taskID string, _ time.Time) (bool, error) {
	f.readyCalls++
	if f.readyErr != nil {
		return false, f.readyErr
	}
	graph, found := f.graphs[graphID]
	if !found {
		return false, nil
	}
	graph.Tasks = append([]domain.ExecutionTask(nil), graph.Tasks...)
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == taskID && graph.Tasks[i].State == domain.TaskBlocked {
			graph.Tasks[i].State = domain.TaskReady
			graph.Revision++
			f.graphs[graphID] = graph
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeExecutionGraphStore) CancelTask(_ context.Context, graphID, taskID string, _ time.Time) (bool, error) {
	f.cancelCalls++
	if f.cancelErr != nil {
		return false, f.cancelErr
	}
	graph, found := f.graphs[graphID]
	if !found {
		return false, nil
	}
	graph.Tasks = append([]domain.ExecutionTask(nil), graph.Tasks...)
	for i := range graph.Tasks {
		task := &graph.Tasks[i]
		if task.ID != taskID || task.Assignment != nil {
			continue
		}
		if task.State != domain.TaskPending && task.State != domain.TaskReady && task.State != domain.TaskBlocked && task.State != domain.TaskFailed {
			return false, nil
		}
		task.State = domain.TaskCancelled
		graph.Revision++
		f.graphs[graphID] = graph
		return true, nil
	}
	return false, nil
}

func executionGraphTestRouter(store ExecutionGraphStore, wake func()) http.Handler {
	r := chi.NewRouter()
	(&ExecutionGraphsController{Store: store, Wake: wake}).Register(r)
	return r
}

func executionGraphFixture() domain.ExecutionGraph {
	return domain.ExecutionGraph{
		SchemaVersion: domain.ExecutionGraphSchemaVersion,
		ID:            "plan-1",
		Revision:      1,
		ProjectID:     "project-1",
		Objective:     "Ship a slice",
		Slices: []domain.ExecutionSlice{{
			ID: "slice-1", Objective: "Ship one increment", Scope: "Source and docs",
			Deliverables: []string{"Reviewed source"}, AcceptanceCriteria: []string{"Checks pass"},
		}},
		Tasks: []domain.ExecutionTask{{
			ID: "task-1", SliceID: "slice-1", Objective: "Implement", Scope: "Source only",
			Deliverables: []string{"Code"}, AcceptanceCriteria: []string{"Focused tests pass"},
			Dependencies: []string{}, Priority: 80, RecommendedCapabilities: []string{"agent"},
			PreferredAgent: "codex", FallbackAgents: []string{"claude-code"}, Resources: []string{"src"},
			IntegrationRequirements: []string{"Build succeeds"}, ValidationProcedures: []string{"Run focused tests"},
			Complexity: "medium", State: domain.TaskReady, Attempts: []domain.TaskAttempt{},
		}},
	}
}

func requestJSON(t *testing.T, method, path string, value any) *http.Request {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequest(method, path, bytes.NewReader(data))
}

func TestExecutionGraphImportReadAndList(t *testing.T) {
	store := &fakeExecutionGraphStore{}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	graph := executionGraphFixture()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPost, "/execution-graphs", graph))
	if rec.Code != http.StatusCreated || store.saves != 1 || wakes != 1 {
		t.Fatalf("import status=%d saves=%d wakes=%d body=%s", rec.Code, store.saves, wakes, rec.Body.String())
	}
	var created ExecutionGraphEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Graph.ID != graph.ID || created.Graph.Tasks[0].AcceptanceCriteria[0] != "Focused tests pass" {
		t.Fatalf("plan fields lost at import: %+v", created.Graph)
	}
	for _, tc := range []struct {
		path string
		code int
	}{
		{"/execution-graphs/plan-1", http.StatusOK},
		{"/execution-graphs/missing", http.StatusNotFound},
		{"/execution-graphs", http.StatusOK},
	} {
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.code {
			t.Fatalf("GET %s status=%d body=%s", tc.path, rec.Code, rec.Body.String())
		}
	}
}

func TestPrepareExecutionGraphNeverWakesAndRetriesIdempotently(t *testing.T) {
	store := &fakeExecutionGraphStore{}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	graph := executionGraphFixture()
	for i, want := range []int{http.StatusCreated, http.StatusOK} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, requestJSON(t, http.MethodPost, "/execution-graphs/prepared", graph))
		if rec.Code != want || wakes != 0 || len(store.graphs) != 0 || len(store.prepared) != 1 {
			t.Fatalf("prepare %d status=%d wakes=%d active=%d prepared=%d body=%s", i, rec.Code, wakes, len(store.graphs), len(store.prepared), rec.Body.String())
		}
	}
	changed := graph
	changed.Objective = "Changed objective"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPost, "/execution-graphs/prepared", changed))
	if rec.Code != http.StatusConflict || wakes != 0 {
		t.Fatalf("changed prepare status=%d wakes=%d body=%s", rec.Code, wakes, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodGet, "/execution-graphs", nil))
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), graph.ID) {
		t.Fatalf("prepared graph appeared in active listing: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestExecutionGraphImportRejectsBadPlanAndJSON(t *testing.T) {
	store := &fakeExecutionGraphStore{}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	invalid := executionGraphFixture()
	invalid.Tasks[0].AcceptanceCriteria = nil
	wrongRevision := executionGraphFixture()
	wrongRevision.Revision = 2
	unsupportedCapability := executionGraphFixture()
	unsupportedCapability.Tasks[0].RecommendedCapabilities = []string{"coding"}
	for _, tc := range []struct {
		name string
		req  *http.Request
	}{
		{"missing acceptance", requestJSON(t, http.MethodPost, "/execution-graphs", invalid)},
		{"wrong first revision", requestJSON(t, http.MethodPost, "/execution-graphs", wrongRevision)},
		{"unsupported capability", requestJSON(t, http.MethodPost, "/execution-graphs", unsupportedCapability)},
		{"unknown field", httptest.NewRequest(http.MethodPost, "/execution-graphs", strings.NewReader(`{"id":"plan-1","unknown":true}`))},
		{"trailing body", httptest.NewRequest(http.MethodPost, "/execution-graphs", strings.NewReader(`{} {}`))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, tc.req)
			if rec.Code != http.StatusBadRequest || store.saves != 0 || wakes != 0 {
				t.Fatalf("status=%d saves=%d wakes=%d body=%s", rec.Code, store.saves, wakes, rec.Body.String())
			}
		})
	}
}

func TestExecutionGraphUpdateRejectsUnsupportedAgentContract(t *testing.T) {
	previous := executionGraphFixture()
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{previous.ID: previous}}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	next := executionGraphFixture()
	next.Revision = 2
	next.Tasks[0].RecommendedCapabilities = []string{"coding"}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPut, "/execution-graphs/plan-1", next))
	if rec.Code != http.StatusBadRequest || store.saves != 0 || wakes != 0 {
		t.Fatalf("status=%d saves=%d wakes=%d body=%s", rec.Code, store.saves, wakes, rec.Body.String())
	}
}

func TestExecutionGraphUpdateChecksContinuityAndConflict(t *testing.T) {
	previous := executionGraphFixture()
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{previous.ID: previous}}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	next := executionGraphFixture()
	next.Revision = 2
	next.Tasks[0].Scope = "Source and supporting notes"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPut, "/execution-graphs/plan-1", next))
	if rec.Code != http.StatusOK || store.saves != 1 || wakes != 1 || store.graphs["plan-1"].Revision != 2 {
		t.Fatalf("update status=%d saves=%d wakes=%d body=%s", rec.Code, store.saves, wakes, rec.Body.String())
	}
	// A repeated revision cannot silently overwrite the accepted update.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPut, "/execution-graphs/plan-1", next))
	if rec.Code != http.StatusConflict {
		t.Fatalf("stale revision status=%d body=%s", rec.Code, rec.Body.String())
	}
	wrongID := next
	wrongID.ID = "different"
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPut, "/execution-graphs/plan-1", wrongID))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("mismatched id status=%d body=%s", rec.Code, rec.Body.String())
	}
	store.saveErr = execution.ErrGraphConflict
	newer := next
	newer.Revision = 3
	newer.Tasks[0].Scope = "Source, notes, and examples"
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPut, "/execution-graphs/plan-1", newer))
	if rec.Code != http.StatusConflict || wakes != 1 {
		t.Fatalf("write conflict status=%d wakes=%d body=%s", rec.Code, wakes, rec.Body.String())
	}
}

func TestExecutionGraphRuntimeFactsAreBadRequests(t *testing.T) {
	graph := executionGraphFixture()
	store := &fakeExecutionGraphStore{saveErr: execution.ErrGraphRuntimeFacts}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPost, "/execution-graphs", graph))
	if rec.Code != http.StatusBadRequest || store.saves != 0 || wakes != 0 {
		t.Fatalf("import status=%d saves=%d wakes=%d body=%s", rec.Code, store.saves, wakes, rec.Body.String())
	}
	store.graphs = map[string]domain.ExecutionGraph{graph.ID: graph}
	next := executionGraphFixture()
	next.Revision = 2
	next.Tasks[0].Scope = "Updated plan scope"
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPut, "/execution-graphs/plan-1", next))
	if rec.Code != http.StatusBadRequest || store.saves != 0 || wakes != 0 {
		t.Fatalf("update status=%d saves=%d wakes=%d body=%s", rec.Code, store.saves, wakes, rec.Body.String())
	}
}

func TestExecutionGraphUnknownProjectIsNotFound(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/execution-graphs", nil)
	graphWriteError(recorder, request, execution.ErrGraphProjectNotFound)
	if recorder.Code != http.StatusNotFound || !strings.Contains(recorder.Body.String(), "PROJECT_NOT_FOUND") {
		t.Fatalf("unknown project response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestExecutionGraphReviewRequiresExplicitCurrentAttempt(t *testing.T) {
	graph := executionGraphFixture()
	graph.Tasks[0].State = domain.TaskReview
	graph.Tasks[0].Assignment = &domain.TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
	graph.Tasks[0].Attempts = []domain.TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{graph.ID: graph}}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	path := "/execution-graphs/plan-1/tasks/task-1/review"
	for _, tc := range []struct {
		decision string
		wantWake int
	}{
		{"accept", 1}, {"revise", 2}, {"reject", 2},
	} {
		store.graphs[graph.ID] = graph
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, requestJSON(t, http.MethodPost, path, ReviewExecutionTaskRequest{AttemptID: "attempt-1", Decision: tc.decision}))
		if rec.Code != http.StatusNoContent || wakes != tc.wantWake {
			t.Fatalf("review %s status=%d wakes=%d body=%s", tc.decision, rec.Code, wakes, rec.Body.String())
		}
	}
	for _, tc := range []struct {
		name   string
		body   ReviewExecutionTaskRequest
		want   int
		mutate func()
	}{
		{"invalid decision", ReviewExecutionTaskRequest{AttemptID: "attempt-1", Decision: "approve"}, http.StatusBadRequest, nil},
		{"stale attempt", ReviewExecutionTaskRequest{AttemptID: "old-attempt", Decision: "accept"}, http.StatusConflict, nil},
		{"wrong state", ReviewExecutionTaskRequest{AttemptID: "attempt-1", Decision: "accept"}, http.StatusConflict, func() {
			g := store.graphs[graph.ID]
			g.Tasks[0].State = domain.TaskRunning
			store.graphs[graph.ID] = g
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store.graphs[graph.ID] = graph
			if tc.mutate != nil {
				tc.mutate()
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, requestJSON(t, http.MethodPost, path, tc.body))
			if rec.Code != tc.want {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestExecutionGraphReviewCarriesBoundedEvidence(t *testing.T) {
	graph := executionGraphFixture()
	graph.Tasks[0].State = domain.TaskReview
	graph.Tasks[0].Assignment = &domain.TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
	graph.Tasks[0].Attempts = []domain.TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{graph.ID: graph}}
	router := executionGraphTestRouter(store, nil)
	path := "/execution-graphs/plan-1/tasks/task-1/review"
	req := ReviewExecutionTaskRequest{
		AttemptID: "attempt-1", Decision: "accept",
		SourceHead:        "0123456789abcdef0123456789abcdef01234567",
		PRURL:             "https://github.com/sol-codex/agent-orchestrator/pull/4",
		ValidationSummary: "Independent focused tests passed.",
		EvidenceRefs:      []string{"https://github.com/sol-codex/agent-orchestrator/actions/runs/1"},
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPost, path, req))
	if rec.Code != http.StatusNoContent || store.evidence.SourceHead != req.SourceHead ||
		store.evidence.PRURL != req.PRURL || store.evidence.ValidationSummary != req.ValidationSummary ||
		len(store.evidence.EvidenceRefs) != 1 {
		t.Fatalf("review evidence status=%d evidence=%+v", rec.Code, store.evidence)
	}
	store.graphs[graph.ID] = graph
	req.SourceHead = "short"
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, requestJSON(t, http.MethodPost, path, req))
	if rec.Code != http.StatusBadRequest || store.reviewCalls != 1 {
		t.Fatalf("invalid evidence status=%d calls=%d", rec.Code, store.reviewCalls)
	}
}

func TestExecutionGraphUnblockRequiresBlockedTask(t *testing.T) {
	graph := executionGraphFixture()
	graph.Tasks[0].State = domain.TaskBlocked
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{graph.ID: graph}}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	path := "/execution-graphs/plan-1/tasks/task-1/unblock"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusNoContent || wakes != 1 || store.graphs[graph.ID].Tasks[0].State != domain.TaskReady {
		t.Fatalf("unblock status=%d wakes=%d body=%s", rec.Code, wakes, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusConflict || wakes != 1 || store.readyCalls != 1 {
		t.Fatalf("repeat status=%d wakes=%d readyCalls=%d", rec.Code, wakes, store.readyCalls)
	}
	store.graphs[graph.ID] = graph
	store.readyErr = execution.ErrGraphConflict
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusConflict || wakes != 1 {
		t.Fatalf("write conflict status=%d wakes=%d", rec.Code, wakes)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/execution-graphs/missing/tasks/task-1/unblock", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing graph status=%d", rec.Code)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/execution-graphs/plan-1/tasks/missing/unblock", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing task status=%d", rec.Code)
	}
}

func TestExecutionGraphReviewRevisionLimitConflicts(t *testing.T) {
	graph := executionGraphFixture()
	graph.Tasks[0].State = domain.TaskReview
	graph.Tasks[0].Assignment = &domain.TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
	graph.Tasks[0].Attempts = []domain.TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{graph.ID: graph}, reviewErr: execution.ErrReviewAttemptLimit}
	wakes := 0
	rec := httptest.NewRecorder()
	executionGraphTestRouter(store, func() { wakes++ }).ServeHTTP(rec, requestJSON(t, http.MethodPost, "/execution-graphs/plan-1/tasks/task-1/review", ReviewExecutionTaskRequest{AttemptID: "attempt-1", Decision: "revise"}))
	if rec.Code != http.StatusConflict || wakes != 0 {
		t.Fatalf("status=%d wakes=%d body=%s", rec.Code, wakes, rec.Body.String())
	}
}

func TestExecutionGraphCancelOnlyUnassignedUnfinishedTask(t *testing.T) {
	graph := executionGraphFixture()
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{graph.ID: graph}}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	path := "/execution-graphs/plan-1/tasks/task-1/cancel"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusNoContent || wakes != 1 || store.graphs[graph.ID].Tasks[0].State != domain.TaskCancelled {
		t.Fatalf("cancel status=%d wakes=%d body=%s", rec.Code, wakes, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusConflict || store.cancelCalls != 1 || wakes != 1 {
		t.Fatalf("repeat status=%d calls=%d wakes=%d", rec.Code, store.cancelCalls, wakes)
	}
	active := graph
	active.Tasks = append([]domain.ExecutionTask(nil), graph.Tasks...)
	active.Tasks[0].State = domain.TaskRunning
	active.Tasks[0].Assignment = &domain.TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
	store.graphs[graph.ID] = active
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusConflict || store.cancelCalls != 1 || wakes != 1 {
		t.Fatalf("active status=%d calls=%d wakes=%d", rec.Code, store.cancelCalls, wakes)
	}
	store.graphs[graph.ID] = graph
	store.cancelErr = execution.ErrGraphConflict
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusConflict || wakes != 1 {
		t.Fatalf("write conflict status=%d wakes=%d", rec.Code, wakes)
	}
}

func TestExecutionGraphReconcileAndStorageError(t *testing.T) {
	graph := executionGraphFixture()
	store := &fakeExecutionGraphStore{graphs: map[string]domain.ExecutionGraph{graph.ID: graph}}
	wakes := 0
	router := executionGraphTestRouter(store, func() { wakes++ })
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/execution-graphs/plan-1/reconcile", nil))
	if rec.Code != http.StatusAccepted || wakes != 1 {
		t.Fatalf("reconcile status=%d wakes=%d", rec.Code, wakes)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/execution-graphs/missing/reconcile", nil))
	if rec.Code != http.StatusNotFound || wakes != 1 {
		t.Fatalf("missing reconcile status=%d wakes=%d", rec.Code, wakes)
	}
	store.getErr = errors.New("storage unavailable")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/execution-graphs/plan-1", nil))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "storage unavailable") {
		t.Fatalf("storage failure status=%d body=%s", rec.Code, rec.Body.String())
	}
}
