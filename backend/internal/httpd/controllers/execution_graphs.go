package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
)

// ExecutionGraphStore is the durable plan and review boundary. The store owns
// revision compare-and-swap and atomic task transitions; HTTP only validates
// request shape and wakes the scheduler after a successful write.
type ExecutionGraphStore interface {
	SaveExecutionGraph(context.Context, domain.ExecutionGraph) error
	GetExecutionGraph(context.Context, string) (domain.ExecutionGraph, bool, error)
	ListActiveGraphs(context.Context) ([]domain.ExecutionGraph, error)
	ResolveReview(context.Context, string, string, string, execution.ReviewDecision, time.Time) (bool, error)
	MarkReady(context.Context, string, string, time.Time) (bool, error)
	CancelTask(context.Context, string, string, time.Time) (bool, error)
}

// ExecutionGraphsController serves versioned plan imports and explicit review.
type ExecutionGraphsController struct {
	Store ExecutionGraphStore
	Wake  func()
	Clock func() time.Time
}

func (c *ExecutionGraphsController) Register(r chi.Router) {
	r.Get("/execution-graphs", c.list)
	r.Post("/execution-graphs", c.create)
	r.Get("/execution-graphs/{graphId}", c.get)
	r.Put("/execution-graphs/{graphId}", c.update)
	r.Post("/execution-graphs/{graphId}/reconcile", c.reconcile)
	r.Post("/execution-graphs/{graphId}/tasks/{taskId}/review", c.review)
	r.Post("/execution-graphs/{graphId}/tasks/{taskId}/unblock", c.unblock)
	r.Post("/execution-graphs/{graphId}/tasks/{taskId}/cancel", c.cancel)
}

func (c *ExecutionGraphsController) create(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/execution-graphs")
		return
	}
	var graph domain.ExecutionGraph
	if !decodeGraphRequest(w, r, &graph) {
		return
	}
	if graph.Revision != 1 {
		graphBadRequest(w, r, "An imported execution graph must start at revision 1")
		return
	}
	if err := domain.ValidateExecutionGraph(graph); err != nil {
		graphBadRequest(w, r, err.Error())
		return
	}
	if err := execution.ValidateSupportedAgentContract(graph); err != nil {
		graphBadRequest(w, r, err.Error())
		return
	}
	if err := c.Store.SaveExecutionGraph(r.Context(), graph); err != nil {
		graphWriteError(w, r, err)
		return
	}
	c.wake()
	envelope.WriteJSON(w, http.StatusCreated, ExecutionGraphEnvelope{Graph: graph})
}

func (c *ExecutionGraphsController) get(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodGet, "/api/v1/execution-graphs/{graphId}")
		return
	}
	graph, ok, err := c.Store.GetExecutionGraph(r.Context(), chi.URLParam(r, "graphId"))
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if !ok {
		graphNotFound(w, r)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, ExecutionGraphEnvelope{Graph: graph})
}

func (c *ExecutionGraphsController) list(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodGet, "/api/v1/execution-graphs")
		return
	}
	graphs, err := c.Store.ListActiveGraphs(r.Context())
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if graphs == nil {
		graphs = []domain.ExecutionGraph{}
	}
	envelope.WriteJSON(w, http.StatusOK, ListExecutionGraphsResponse{Graphs: graphs})
}

func (c *ExecutionGraphsController) update(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodPut, "/api/v1/execution-graphs/{graphId}")
		return
	}
	var graph domain.ExecutionGraph
	if !decodeGraphRequest(w, r, &graph) {
		return
	}
	id := chi.URLParam(r, "graphId")
	if graph.ID != id {
		graphBadRequest(w, r, "Graph ID in body must match the path")
		return
	}
	previous, ok, err := c.Store.GetExecutionGraph(r.Context(), id)
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if !ok {
		graphNotFound(w, r)
		return
	}
	if graph.Revision != previous.Revision+1 {
		graphConflict(w, r, "Execution graph revision is stale")
		return
	}
	if err := domain.ValidateExecutionGraphTransition(previous, graph); err != nil {
		graphBadRequest(w, r, err.Error())
		return
	}
	if err := execution.ValidateSupportedAgentContract(graph); err != nil {
		graphBadRequest(w, r, err.Error())
		return
	}
	if err := c.Store.SaveExecutionGraph(r.Context(), graph); err != nil {
		graphWriteError(w, r, err)
		return
	}
	c.wake()
	envelope.WriteJSON(w, http.StatusOK, ExecutionGraphEnvelope{Graph: graph})
}

func (c *ExecutionGraphsController) reconcile(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil || c.Wake == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/execution-graphs/{graphId}/reconcile")
		return
	}
	_, ok, err := c.Store.GetExecutionGraph(r.Context(), chi.URLParam(r, "graphId"))
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if !ok {
		graphNotFound(w, r)
		return
	}
	c.Wake()
	w.WriteHeader(http.StatusAccepted)
}

func (c *ExecutionGraphsController) review(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/execution-graphs/{graphId}/tasks/{taskId}/review")
		return
	}
	var request ReviewExecutionTaskRequest
	if err := decodeExecutionJSON(w, r, &request); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
		return
	}
	if strings.TrimSpace(request.AttemptID) == "" || request.AttemptID != strings.TrimSpace(request.AttemptID) {
		graphBadRequest(w, r, "attemptId must be nonempty and trimmed")
		return
	}
	decision := execution.ReviewDecision(request.Decision)
	if decision != execution.ReviewAccept && decision != execution.ReviewRevise && decision != execution.ReviewReject {
		graphBadRequest(w, r, "decision must be accept, revise, or reject")
		return
	}
	graphID, taskID := chi.URLParam(r, "graphId"), chi.URLParam(r, "taskId")
	graph, found, err := c.Store.GetExecutionGraph(r.Context(), graphID)
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if !found {
		graphNotFound(w, r)
		return
	}
	var task *domain.ExecutionTask
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == taskID {
			task = &graph.Tasks[i]
			break
		}
	}
	if task == nil {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_TASK_NOT_FOUND", "Execution task not found", nil)
		return
	}
	if task.State != domain.TaskReview {
		graphConflict(w, r, "Task is not awaiting review")
		return
	}
	changed, err := c.Store.ResolveReview(r.Context(), graphID, taskID, request.AttemptID, decision, c.now())
	if err != nil {
		graphWriteError(w, r, err)
		return
	}
	if !changed {
		graphConflict(w, r, "Review attempt is stale or task state changed")
		return
	}
	if decision == execution.ReviewAccept || decision == execution.ReviewRevise {
		c.wake()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *ExecutionGraphsController) unblock(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/execution-graphs/{graphId}/tasks/{taskId}/unblock")
		return
	}
	graphID, taskID := chi.URLParam(r, "graphId"), chi.URLParam(r, "taskId")
	graph, found, err := c.Store.GetExecutionGraph(r.Context(), graphID)
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if !found {
		graphNotFound(w, r)
		return
	}
	var task *domain.ExecutionTask
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == taskID {
			task = &graph.Tasks[i]
			break
		}
	}
	if task == nil {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_TASK_NOT_FOUND", "Execution task not found", nil)
		return
	}
	if task.State != domain.TaskBlocked {
		graphConflict(w, r, "Task is not blocked")
		return
	}
	changed, err := c.Store.MarkReady(r.Context(), graphID, taskID, c.now())
	if err != nil {
		graphWriteError(w, r, err)
		return
	}
	if !changed {
		graphConflict(w, r, "Task blocker, dependencies, or state changed")
		return
	}
	c.wake()
	w.WriteHeader(http.StatusNoContent)
}

func (c *ExecutionGraphsController) cancel(w http.ResponseWriter, r *http.Request) {
	if c.Store == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/execution-graphs/{graphId}/tasks/{taskId}/cancel")
		return
	}
	graphID, taskID := chi.URLParam(r, "graphId"), chi.URLParam(r, "taskId")
	graph, found, err := c.Store.GetExecutionGraph(r.Context(), graphID)
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	if !found {
		graphNotFound(w, r)
		return
	}
	var task *domain.ExecutionTask
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == taskID {
			task = &graph.Tasks[i]
			break
		}
	}
	if task == nil {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_TASK_NOT_FOUND", "Execution task not found", nil)
		return
	}
	if task.Assignment != nil || (task.State != domain.TaskPending && task.State != domain.TaskReady && task.State != domain.TaskBlocked && task.State != domain.TaskFailed) {
		graphConflict(w, r, "Only unassigned unfinished tasks can be cancelled")
		return
	}
	changed, err := c.Store.CancelTask(r.Context(), graphID, taskID, c.now())
	if err != nil {
		graphWriteError(w, r, err)
		return
	}
	if !changed {
		graphConflict(w, r, "Task assignment or state changed")
		return
	}
	c.wake()
	w.WriteHeader(http.StatusNoContent)
}

func (c *ExecutionGraphsController) wake() {
	if c.Wake != nil {
		c.Wake()
	}
}

func (c *ExecutionGraphsController) now() time.Time {
	if c.Clock != nil {
		return c.Clock().UTC()
	}
	return time.Now().UTC()
}

func decodeGraphRequest(w http.ResponseWriter, r *http.Request, graph *domain.ExecutionGraph) bool {
	if err := decodeExecutionJSON(w, r, graph); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
		return false
	}
	return true
}

func decodeExecutionJSON(w http.ResponseWriter, r *http.Request, out any) error {
	const maxGraphBodyBytes = 4 << 20
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGraphBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func graphBadRequest(w http.ResponseWriter, r *http.Request, message string) {
	envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_EXECUTION_GRAPH", message, nil)
}

func graphNotFound(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_GRAPH_NOT_FOUND", "Execution graph not found", nil)
}

func graphConflict(w http.ResponseWriter, r *http.Request, message string) {
	envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "EXECUTION_GRAPH_CONFLICT", message, nil)
}

func graphWriteError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, execution.ErrGraphConflict) {
		graphConflict(w, r, "Execution graph revision or task state changed")
		return
	}
	if errors.Is(err, execution.ErrGraphRuntimeFacts) {
		graphBadRequest(w, r, "Task runtime state and attempts are managed by the execution scheduler")
		return
	}
	if errors.Is(err, execution.ErrReviewAttemptLimit) {
		graphConflict(w, r, "Task revision attempt limit reached")
		return
	}
	if errors.Is(err, execution.ErrInvalidDecision) {
		graphBadRequest(w, r, "Invalid review decision")
		return
	}
	// The error capture middleware records unexpected storage failures while the
	// response keeps the locked API error envelope.
	envelope.WriteError(w, r, err)
}
