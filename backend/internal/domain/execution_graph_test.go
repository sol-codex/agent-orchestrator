package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validExecutionGraph() ExecutionGraph {
	return ExecutionGraph{
		SchemaVersion: ExecutionGraphSchemaVersion,
		ID:            "plan-1",
		Revision:      1,
		ProjectID:     ProjectID("project-1"),
		Objective:     "Deliver the next release",
		Slices: []ExecutionSlice{{
			ID:                 "slice-1",
			Objective:          "Ship a usable increment",
			Scope:              "One feature",
			Deliverables:       []string{"Change and review"},
			AcceptanceCriteria: []string{"Feature behaves as specified"},
		}},
		Tasks: []ExecutionTask{
			{
				ID:                      "task-1",
				SliceID:                 "slice-1",
				Objective:               "Define the contract",
				Scope:                   "Contract only",
				Deliverables:            []string{"Contract"},
				AcceptanceCriteria:      []string{"Contract reviewed"},
				Priority:                10,
				RecommendedCapabilities: []string{"architecture"},
				PreferredAgent:          "codex",
				FallbackAgents:          []string{"claude"},
				Resources:               []string{"backend/internal/domain"},
				IntegrationRequirements: []string{"Stable wire shape"},
				ValidationProcedures:    []string{"Run domain tests"},
				Complexity:              "medium",
				State:                   TaskCompleted,
			},
			{
				ID:                   "task-2",
				SliceID:              "slice-1",
				Objective:            "Implement the contract",
				Scope:                "Implementation only",
				Deliverables:         []string{"Implementation"},
				AcceptanceCriteria:   []string{"Tests pass"},
				Dependencies:         []string{"task-1"},
				SoftDependencies:     []string{"task-3"},
				Resources:            []string{"backend/internal/service"},
				ValidationProcedures: []string{"Run service tests"},
				Complexity:           "high",
				State:                TaskReady,
			},
			{
				ID:                   "task-3",
				SliceID:              "slice-1",
				Objective:            "Write independent docs",
				Scope:                "Docs only",
				Deliverables:         []string{"Documentation"},
				AcceptanceCriteria:   []string{"Examples are accurate"},
				Resources:            []string{"docs"},
				ValidationProcedures: []string{"Review examples"},
				Complexity:           "low",
				State:                TaskPending,
			},
		},
	}
}

func TestValidateExecutionGraph(t *testing.T) {
	base := validExecutionGraph()
	if err := ValidateExecutionGraph(base); err != nil {
		t.Fatalf("valid graph: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*ExecutionGraph)
		wantErr string
	}{
		{"version", func(g *ExecutionGraph) { g.SchemaVersion = 2 }, "schema version"},
		{"revision", func(g *ExecutionGraph) { g.Revision = 0 }, "revision"},
		{"duplicate slice", func(g *ExecutionGraph) { g.Slices = append(g.Slices, g.Slices[0]) }, "duplicate slice"},
		{"duplicate task", func(g *ExecutionGraph) { g.Tasks = append(g.Tasks, g.Tasks[0]) }, "duplicate task"},
		{"unknown slice", func(g *ExecutionGraph) { g.Tasks[1].SliceID = "missing" }, "unknown slice"},
		{"missing scope", func(g *ExecutionGraph) { g.Tasks[1].Scope = "" }, "scope"},
		{"missing deliverable", func(g *ExecutionGraph) { g.Tasks[1].Deliverables = nil }, "deliverables"},
		{"missing acceptance", func(g *ExecutionGraph) { g.Tasks[1].AcceptanceCriteria = nil }, "acceptanceCriteria"},
		{"missing validation", func(g *ExecutionGraph) { g.Tasks[1].ValidationProcedures = nil }, "validationProcedures"},
		{"unknown hard dependency", func(g *ExecutionGraph) { g.Tasks[1].Dependencies = []string{"missing"} }, "unknown dependency"},
		{"unknown soft dependency", func(g *ExecutionGraph) { g.Tasks[1].SoftDependencies = []string{"missing"} }, "unknown dependency"},
		{"duplicate dependency", func(g *ExecutionGraph) { g.Tasks[1].SoftDependencies = []string{"task-1"} }, "duplicate dependency"},
		{"self dependency", func(g *ExecutionGraph) { g.Tasks[1].Dependencies = []string{"task-2"} }, "self dependency"},
		{"hard cycle", func(g *ExecutionGraph) {
			g.Tasks[0].State = TaskPending
			g.Tasks[0].Dependencies = []string{"task-2"}
			g.Tasks[1].State = TaskPending
		}, "cycle"},
		{"ready before dependency", func(g *ExecutionGraph) { g.Tasks[0].State = TaskPending }, "not completed"},
		{"invalid state", func(g *ExecutionGraph) { g.Tasks[1].State = "unknown" }, "invalid state"},
		{"missing active assignment", func(g *ExecutionGraph) { g.Tasks[1].State = TaskAssigned }, "requires agent"},
		{"missing recorded active attempt", func(g *ExecutionGraph) {
			g.Tasks[1].State = TaskAssigned
			g.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
		}, "must reference a recorded attempt"},
		{"mismatched active attempt agent", func(g *ExecutionGraph) {
			g.Tasks[1].State = TaskAssigned
			g.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
			g.Tasks[1].Attempts = []TaskAttempt{{ID: "attempt-1", AgentID: "claude"}}
		}, "agent differs"},
		{"finished active attempt", func(g *ExecutionGraph) {
			finishedAt := time.Unix(100, 0)
			g.Tasks[1].State = TaskRunning
			g.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
			g.Tasks[1].Attempts = []TaskAttempt{{ID: "attempt-1", AgentID: "codex", FinishedAt: &finishedAt}}
		}, "finished attempt"},
		{"duplicate resource", func(g *ExecutionGraph) { g.Tasks[1].Resources = []string{"service", "service"} }, "duplicate"},
		{"resource conflict", func(g *ExecutionGraph) {
			g.Tasks[1].State = TaskAssigned
			g.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
			g.Tasks[1].Attempts = []TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
			g.Tasks[2].State = TaskRunning
			g.Tasks[2].Assignment = &TaskAssignment{AgentID: "claude", AttemptID: "attempt-2"}
			g.Tasks[2].Attempts = []TaskAttempt{{ID: "attempt-2", AgentID: "claude"}}
			g.Tasks[2].Resources = g.Tasks[1].Resources
		}, "exclusive resource"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := validExecutionGraph()
			tt.mutate(&graph)
			if err := ValidateExecutionGraph(graph); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateExecutionGraph() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestExecutionGraphPlanContinuityJSON(t *testing.T) {
	graph := validExecutionGraph()
	encoded, err := json.Marshal(graph)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ExecutionGraph
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionGraph(decoded); err != nil {
		t.Fatalf("decoded graph: %v", err)
	}
	if decoded.ID != graph.ID || decoded.Tasks[1].ID != graph.Tasks[1].ID ||
		decoded.Tasks[1].Dependencies[0] != "task-1" ||
		decoded.Tasks[1].AcceptanceCriteria[0] != "Tests pass" {
		t.Fatalf("plan fields did not survive round trip: %+v", decoded.Tasks[1])
	}
}

func TestValidateExecutionGraphTransition(t *testing.T) {
	previous := validExecutionGraph()
	next := cloneExecutionGraph(t, previous)
	next.Revision++
	next.Tasks[1].State = TaskAssigned
	next.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", Provider: "openai", AttemptID: "attempt-1"}
	next.Tasks[1].Attempts = []TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
	next.Tasks[2].Scope = "Replanned docs scope"
	if err := ValidateExecutionGraphTransition(previous, next); err != nil {
		t.Fatalf("valid revised graph: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*ExecutionGraph)
		wantErr string
	}{
		{"revision does not advance", func(g *ExecutionGraph) { g.Revision = previous.Revision }, "revision must advance"},
		{"graph id changed", func(g *ExecutionGraph) { g.ID = "other" }, "identity changed"},
		{"slice removed", func(g *ExecutionGraph) {
			g.Slices[0].ID = "replacement"
			for i := range g.Tasks {
				g.Tasks[i].SliceID = "replacement"
			}
		}, "slice"},
		{"task removed", func(g *ExecutionGraph) {
			g.Tasks[1].SoftDependencies = nil
			g.Tasks = g.Tasks[:2]
		}, "was removed"},
		{"task moved", func(g *ExecutionGraph) {
			g.Slices = append(g.Slices, ExecutionSlice{
				ID: "slice-2", Objective: "Another slice", Scope: "Another scope",
				Deliverables: []string{"New deliverable"}, AcceptanceCriteria: []string{"Accepted"},
			})
			g.Tasks[1].SliceID = "slice-2"
		}, "slice identity changed"},
		{"illegal state edge", func(g *ExecutionGraph) {
			g.Tasks[1].State = TaskCompleted
			g.Tasks[1].Assignment = nil
		}, "invalid transition"},
		{"completed work edited", func(g *ExecutionGraph) { g.Tasks[0].AcceptanceCriteria = []string{"Changed criterion"} }, "accepted completed work changed"},
		{"attempt history erased", func(g *ExecutionGraph) {
			g.Tasks[1].Attempts = g.Tasks[1].Attempts[1:]
		}, "attempt history changed"},
		{"finished retry disposition edited", func(g *ExecutionGraph) {
			g.Tasks[1].Attempts[0].Retryable = true
		}, "finished attempt changed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := cloneExecutionGraph(t, previous)
			candidate := cloneExecutionGraph(t, next)
			if tt.name == "attempt history erased" || tt.name == "finished retry disposition edited" {
				finishedAt := time.Unix(100, 0)
				old.Tasks[1].Attempts = []TaskAttempt{{ID: "old-attempt", AgentID: "claude", FinishedAt: &finishedAt}}
				candidate.Tasks[1].Attempts = append([]TaskAttempt(nil), old.Tasks[1].Attempts...)
				candidate.Tasks[1].Attempts = append(candidate.Tasks[1].Attempts, TaskAttempt{ID: "attempt-1", AgentID: "codex"})
			}
			tt.mutate(&candidate)
			if err := ValidateExecutionGraphTransition(old, candidate); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateExecutionGraphTransition() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestExecutionGraphTransitionCanFinishCurrentAttempt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     TaskState
		retryable bool
	}{
		{"successful output awaits review", TaskReview, false},
		{"transient worker failure is retryable", TaskFailed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := validExecutionGraph()
			previous.Tasks[1].State = TaskRunning
			previous.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", AttemptID: "attempt-1"}
			previous.Tasks[1].Attempts = []TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
			next := cloneExecutionGraph(t, previous)
			next.Revision++
			next.Tasks[1].State = tc.state
			finishedAt := time.Unix(100, 0)
			next.Tasks[1].Attempts[0].FinishedAt = &finishedAt
			next.Tasks[1].Attempts[0].Retryable = tc.retryable
			if err := ValidateExecutionGraphTransition(previous, next); err != nil {
				t.Fatalf("finishing current attempt: %v", err)
			}
			if got := cloneExecutionGraph(t, next).Tasks[1].Attempts[0].Retryable; got != tc.retryable {
				t.Fatalf("retry disposition after JSON recovery = %t, want %t", got, tc.retryable)
			}
		})
	}
}

func TestExecutionGraphAssignmentProviderSurvivesRestart(t *testing.T) {
	graph := validExecutionGraph()
	graph.Tasks[1].State = TaskAssigned
	graph.Tasks[1].Assignment = &TaskAssignment{AgentID: "codex", Provider: "openai", AttemptID: "attempt-1"}
	graph.Tasks[1].Attempts = []TaskAttempt{{ID: "attempt-1", AgentID: "codex"}}
	if err := ValidateExecutionGraph(graph); err != nil {
		t.Fatal(err)
	}
	recovered := cloneExecutionGraph(t, graph)
	if recovered.Tasks[1].Assignment == nil || recovered.Tasks[1].Assignment.Provider != "openai" {
		t.Fatalf("provider lost during JSON recovery: %+v", recovered.Tasks[1].Assignment)
	}
	recovered.Revision++
	recovered.Tasks[1].State = TaskRunning
	recovered.Tasks[1].Assignment.Provider = "anthropic"
	if err := ValidateExecutionGraphTransition(graph, recovered); err == nil || !strings.Contains(err.Error(), "active assignment changed") {
		t.Fatalf("provider switch during active claim error = %v", err)
	}
}

func cloneExecutionGraph(t *testing.T, graph ExecutionGraph) ExecutionGraph {
	t.Helper()
	encoded, err := json.Marshal(graph)
	if err != nil {
		t.Fatal(err)
	}
	var clone ExecutionGraph
	if err := json.Unmarshal(encoded, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func TestTaskStateTransitions(t *testing.T) {
	allowed := map[TaskState][]TaskState{
		TaskPending:   {TaskReady, TaskBlocked, TaskCancelled},
		TaskReady:     {TaskAssigned, TaskBlocked, TaskCancelled},
		TaskAssigned:  {TaskRunning, TaskReady, TaskBlocked, TaskFailed, TaskCancelled},
		TaskRunning:   {TaskReview, TaskBlocked, TaskFailed, TaskCancelled},
		TaskBlocked:   {TaskReady, TaskRunning, TaskFailed, TaskCancelled},
		TaskReview:    {TaskCompleted, TaskReady, TaskRunning, TaskBlocked, TaskFailed, TaskCancelled},
		TaskFailed:    {TaskReady, TaskCancelled},
		TaskCompleted: {},
		TaskCancelled: {},
	}
	for from, destinations := range allowed {
		for to := range allowed {
			want := false
			for _, candidate := range destinations {
				if candidate == to {
					want = true
				}
			}
			if got := CanTransitionTaskState(from, to); got != want {
				t.Errorf("transition %s -> %s = %t, want %t", from, to, got, want)
			}
		}
	}
	if CanTransitionTaskState("unknown", TaskReady) || CanTransitionTaskState(TaskReady, "unknown") {
		t.Fatal("unknown state transition allowed")
	}
}
