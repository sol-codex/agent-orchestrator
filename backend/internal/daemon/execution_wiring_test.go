package daemon

import (
	"context"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	agentsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/agent"
	execution "github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
)

type executionSessionStoreFake struct {
	records map[domain.SessionID]domain.SessionRecord
	byKey   map[string]domain.SessionID
	reports map[domain.SessionID][]domain.ReportRecord
}

func (f executionSessionStoreFake) GetSessionByClientRequestID(_ context.Context, key string) (domain.SessionRecord, bool, error) {
	id, ok := f.byKey[key]
	return f.records[id], ok, nil
}

func (f executionSessionStoreFake) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	record, ok := f.records[id]
	return record, ok, nil
}

func (f executionSessionStoreFake) ListReportsBySession(_ context.Context, id domain.SessionID) ([]domain.ReportRecord, error) {
	return f.reports[id], nil
}

type executionSpawnFake struct{ cfg ports.SpawnConfig }

func (f *executionSpawnFake) Spawn(_ context.Context, cfg ports.SpawnConfig) (domain.Session, int, int, error) {
	f.cfg = cfg
	return domain.Session{SessionRecord: domain.SessionRecord{ID: "worker-1"}}, 0, 0, nil
}

func TestExecutionSpawnPreservesAttemptAndContract(t *testing.T) {
	spawn := &executionSpawnFake{}
	dispatch := executionSessions{sessions: spawn}
	graph := domain.ExecutionGraph{ID: "graph-1", ProjectID: "hub", Objective: "Ship the wave"}
	task := domain.ExecutionTask{ID: "E2", SliceID: "E", Objective: "Implement turn brief", Scope: "Mapper only", Deliverables: []string{"Typed output"}, AcceptanceCriteria: []string{"No private data"}, Resources: []string{"src/domain/brief.ts"}}
	id, err := dispatch.Spawn(context.Background(), graph, task, execution.Agent{ID: "codex"}, "attempt-1")
	if err != nil || id != "worker-1" {
		t.Fatalf("spawn = %s, %v", id, err)
	}
	if spawn.cfg.ClientRequestID != "attempt-1" || spawn.cfg.ClientRequestHash == "" || spawn.cfg.Harness != domain.HarnessCodex || spawn.cfg.Async {
		t.Fatalf("spawn config lost durable identity or sync boundary: %+v", spawn.cfg)
	}
	for _, required := range []string{"graph-1", "E2", "Typed output", "No private data", "src/domain/brief.ts", "ao report --done --note"} {
		if !strings.Contains(spawn.cfg.Prompt, required) {
			t.Fatalf("prompt omitted %q", required)
		}
	}
}

func TestExecutionLookupFencesUnsettledAndFailedLaunch(t *testing.T) {
	store := executionSessionStoreFake{records: map[domain.SessionID]domain.SessionRecord{"worker-1": {ID: "worker-1"}}, byKey: map[string]domain.SessionID{"attempt-1": "worker-1"}, reports: map[domain.SessionID][]domain.ReportRecord{}}
	dispatch := executionSessions{store: store}
	if _, found, err := dispatch.Lookup(context.Background(), "attempt-1"); err == nil || found {
		t.Fatalf("uncommitted lookup = found %v, %v", found, err)
	}
	record := store.records["worker-1"]
	record.ProvisionState = domain.SessionProvisionFailed
	record.ProvisionError = "launch failed"
	store.records["worker-1"] = record
	if _, found, err := dispatch.Lookup(context.Background(), "attempt-1"); err == nil || found || !strings.Contains(err.Error(), "requiring reconciliation") {
		t.Fatalf("uncommitted failed launch must remain unresolved: found=%v err=%v", found, err)
	}
	record.ProvisionState = domain.SessionProvisionReady
	record.ClientRequestCommitted = true
	store.records["worker-1"] = record
	if id, found, err := dispatch.Lookup(context.Background(), "attempt-1"); err != nil || !found || id != "worker-1" {
		t.Fatalf("committed lookup = %s, %v, %v", id, found, err)
	}
	record.IsTerminated = true
	store.records["worker-1"] = record
	store.reports["worker-1"] = []domain.ReportRecord{{State: domain.ReportDone}}
	if id, found, err := dispatch.Lookup(context.Background(), "attempt-1"); err != nil || !found || id != "worker-1" {
		t.Fatalf("committed terminated worker must be inspected for done report: %s, %v, %v", id, found, err)
	}
	if observation, err := dispatch.Inspect(context.Background(), "worker-1"); err != nil || observation.State != execution.WorkerDone {
		t.Fatalf("committed terminated worker's done report = %+v, %v", observation, err)
	}
}

func TestExecutionInspectDoneRequiresReportAndExitedFails(t *testing.T) {
	store := executionSessionStoreFake{records: map[domain.SessionID]domain.SessionRecord{"worker-1": {ID: "worker-1"}}, reports: map[domain.SessionID][]domain.ReportRecord{}}
	dispatch := executionSessions{store: store}
	if observation, err := dispatch.Inspect(context.Background(), "worker-1"); err != nil || observation.State != execution.WorkerRunning {
		t.Fatalf("active without report = %+v, %v", observation, err)
	}
	record := store.records["worker-1"]
	record.Activity.State = domain.ActivityExited
	store.records["worker-1"] = record
	if observation, err := dispatch.Inspect(context.Background(), "worker-1"); err != nil || observation.State != execution.WorkerFailed {
		t.Fatalf("exited without report = %+v, %v", observation, err)
	}
	store.reports["worker-1"] = []domain.ReportRecord{{State: domain.ReportDone}}
	if observation, err := dispatch.Inspect(context.Background(), "worker-1"); err != nil || observation.State != execution.WorkerDone {
		t.Fatalf("done report = %+v, %v", observation, err)
	}
}

type executionReadinessFake struct{ items agentsvc.Readiness }

func (f executionReadinessFake) CachedReadiness(context.Context) (agentsvc.Readiness, error) {
	return f.items, nil
}

func TestExecutionCapacityUsesObservedInstallation(t *testing.T) {
	items := agentsvc.Readiness{Agents: []domain.AgentReadinessSnapshot{
		{ID: "codex", Installation: domain.AgentInstallationObservation{State: domain.AgentInstallationInstalled}, EffectiveReadiness: domain.AgentReadinessReady},
		{ID: "grok", Installation: domain.AgentInstallationObservation{State: domain.AgentInstallationNotInstalled}, EffectiveReadiness: domain.AgentReadinessNotReady},
	}}
	agents, err := (executionCapacity{agents: executionReadinessFake{items: items}}).Available(context.Background(), "hub")
	if err != nil || len(agents) != 1 || agents[0].ID != "codex" || agents[0].Slots != 4 {
		t.Fatalf("available agents = %+v, %v", agents, err)
	}
}
