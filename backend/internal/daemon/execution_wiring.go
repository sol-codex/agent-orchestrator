package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	agentsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/agent"
	execution "github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// executionSessions binds one durable graph attempt to AO's existing idempotent
// worker spawn. A session row is only adopted once its launch commit exists.
type executionSessions struct {
	store    executionSessionStore
	sessions executionSessionSpawner
}

type executionSessionStore interface {
	GetSessionByClientRequestID(context.Context, string) (domain.SessionRecord, bool, error)
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
	ListReportsBySession(context.Context, domain.SessionID) ([]domain.ReportRecord, error)
}

type executionSessionSpawner interface {
	Spawn(context.Context, ports.SpawnConfig) (domain.Session, int, int, error)
}

func (d executionSessions) Spawn(ctx context.Context, graph domain.ExecutionGraph, task domain.ExecutionTask, agent execution.Agent, attemptID string) (domain.SessionID, error) {
	prompt := executionTaskPrompt(graph, task)
	hash := sha256.Sum256([]byte(prompt))
	session, _, _, err := d.sessions.Spawn(ctx, ports.SpawnConfig{
		ProjectID: graph.ProjectID, Kind: domain.KindWorker,
		Harness: domain.AgentHarness(agent.ID), Prompt: prompt,
		DisplayName: task.Objective, NameFromPrompt: true,
		ClientRequestID: attemptID, ClientRequestHash: "sha256:" + hex.EncodeToString(hash[:]),
	})
	if err != nil {
		return "", err
	}
	return session.ID, nil
}

func (d executionSessions) Lookup(ctx context.Context, attemptID string) (domain.SessionID, bool, error) {
	record, found, err := d.store.GetSessionByClientRequestID(ctx, attemptID)
	if err != nil || !found {
		return "", false, err
	}
	if !record.ClientRequestCommitted && (record.ProvisionState == domain.SessionProvisionFailed || record.IsTerminated) {
		reason := record.ProvisionError
		if reason == "" {
			reason = "worker session failed before launch completed"
		}
		// A terminated or failed seed is not proof that rollback destroyed a
		// runtime. Preserve the graph assignment until a human can verify it.
		return "", false, fmt.Errorf("execution attempt %s has an uncommitted failed launch requiring reconciliation: %s", attemptID, reason)
	}
	if !record.ClientRequestCommitted || record.ProvisionState == domain.SessionProvisionProvisioning {
		return "", false, fmt.Errorf("execution attempt %s has an unsettled session launch", attemptID)
	}
	return record.ID, true, nil
}

func (d executionSessions) Inspect(ctx context.Context, id domain.SessionID) (execution.WorkerObservation, error) {
	record, found, err := d.store.GetSession(ctx, id)
	if err != nil {
		return execution.WorkerObservation{}, err
	}
	if !found {
		return execution.WorkerObservation{State: execution.WorkerUnknown}, nil
	}
	reports, err := d.store.ListReportsBySession(ctx, id)
	if err != nil {
		return execution.WorkerObservation{}, err
	}
	for _, report := range reports {
		if report.State == domain.ReportDone {
			return execution.WorkerObservation{State: execution.WorkerDone}, nil
		}
	}
	if record.ProvisionState == domain.SessionProvisionFailed {
		return execution.WorkerObservation{State: execution.WorkerFailed, Error: record.ProvisionError}, nil
	}
	if record.IsTerminated || record.Activity.State == domain.ActivityExited {
		return execution.WorkerObservation{State: execution.WorkerFailed, Error: "worker session exited without a done report"}, nil
	}
	return execution.WorkerObservation{State: execution.WorkerRunning}, nil
}

func executionTaskPrompt(graph domain.ExecutionGraph, task domain.ExecutionTask) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Execution graph %s, task %s, slice %s.\nProject objective: %s\nTask objective: %s\nScope: %s\n", graph.ID, task.ID, task.SliceID, graph.Objective, task.Objective, task.Scope)
	writeExecutionItems(&b, "Deliverables", task.Deliverables)
	writeExecutionItems(&b, "Acceptance criteria", task.AcceptanceCriteria)
	writeExecutionItems(&b, "Integration requirements", task.IntegrationRequirements)
	writeExecutionItems(&b, "Validation procedures", task.ValidationProcedures)
	fmt.Fprintf(&b, "\nKeep edits within the owned resources: %s. Report blockers promptly. When the deliverables are ready, use `ao report --done --note \"<concise result>\"` and attach output references with --pr-created or --artifact. A done report enters review; it does not accept the task.\n", strings.Join(task.Resources, ", "))
	return b.String()
}

func writeExecutionItems(b *strings.Builder, label string, values []string) {
	if len(values) == 0 {
		return
	}
	b.WriteString(label)
	b.WriteString(":\n")
	for _, value := range values {
		fmt.Fprintf(b, "- %s\n", value)
	}
}

// executionCapacity uses launch-readiness observations from the installed
// adapters. The graph's preferred/fallback order is the coordinator's explicit
// agent choice; this adapter does not invent performance scores.
type executionCapacity struct{ agents executionReadiness }

type executionReadiness interface {
	CachedReadiness(context.Context) (agentsvc.Readiness, error)
}

func (c executionCapacity) Available(ctx context.Context, _ domain.ProjectID) ([]execution.Agent, error) {
	readiness, err := c.agents.CachedReadiness(ctx)
	if err != nil {
		return nil, err
	}
	available := make([]execution.Agent, 0, len(readiness.Agents))
	for _, item := range readiness.Agents {
		if item.Installation.State != domain.AgentInstallationInstalled || item.EffectiveReadiness != domain.AgentReadinessReady {
			continue
		}
		available = append(available, execution.Agent{ID: item.ID, Provider: item.ID,
			Capabilities: []string{"agent", "harness:" + item.ID}, Slots: 4})
	}
	return available, nil
}

var errExecutionReviewPending = errors.New("execution task awaits a recorded acceptance decision")

// Generic acceptance criteria cannot safely be inferred from a worker's own
// report. The API review action records the coordinator's explicit decision.
type executionReviewGate struct{}

func (executionReviewGate) Validate(context.Context, domain.ExecutionGraph, domain.ExecutionTask) (execution.ReviewDecision, error) {
	return "", errExecutionReviewPending
}

func startExecution(ctx context.Context, sessionRecoveryDone <-chan struct{}, store *sqlite.Store, sessions executionSessionSpawner, agents executionReadiness, logger *slog.Logger) (*execution.Service, <-chan struct{}, error) {
	svc, err := execution.New(store, executionSessions{store: store, sessions: sessions}, executionCapacity{agents: agents}, executionReviewGate{}, execution.Config{
		Logger: logger, MaxConcurrent: 4, MaxPerProvider: map[string]int{}, MaxAttempts: execution.DefaultMaxAttempts,
		SpawnLease: 2 * time.Minute, ReconcileInterval: 15 * time.Second,
	})
	if err != nil {
		return nil, nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-sessionRecoveryDone:
		case <-ctx.Done():
			return
		}
		if runErr := svc.Run(ctx, nil); runErr != nil && !errors.Is(runErr, context.Canceled) {
			logger.Warn("execution scheduler stopped", "err", runErr)
		}
	}()
	return svc, done, nil
}
