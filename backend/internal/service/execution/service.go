// Package execution schedules approved project graphs over AO worker sessions.
// The store owns durable claims and state transitions; this package decides
// what is eligible and never treats a worker report as task acceptance.
package execution

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Claim is a proposed assignment. ClaimTask must atomically recheck the task's
// READY state, hard dependencies, exclusive resources and concurrency limits.
// AttemptID is also the stable worker-spawn idempotency key.
type Claim struct {
	GraphID             string
	ContractFingerprint string
	TaskID              string
	AttemptID           string
	AgentID             string
	Provider            string
	Resources           []string
	At                  time.Time
	LeaseExpiresAt      time.Time
	MaxConcurrent       int
	MaxPerProvider      int
}

// ReviewDecision is the validator's disposition, separate from worker status.
type ReviewDecision string

const (
	ReviewAccept       ReviewDecision = "accept"
	ReviewRevise       ReviewDecision = "revise"
	ReviewReject       ReviewDecision = "reject"
	DefaultMaxAttempts                = 2
)

var (
	// ErrGraphConflict means a persisted revision or attempt already changed.
	ErrGraphConflict      = errors.New("execution graph revision conflict")
	ErrInvalidDecision    = errors.New("invalid execution review decision")
	ErrGraphRuntimeFacts  = errors.New("execution graph runtime facts are scheduler-owned")
	ErrReviewAttemptLimit = errors.New("execution review revision attempt limit reached")
)

// Store is the durable execution boundary. Every successful mutation must
// advance the graph revision. Every attempt mutation must fence by graph,
// task and attempt IDs; false means another actor already advanced it.
// ClaimTask must use one transaction to recheck READY, completed hard deps,
// global/provider limits, and exclusive resources across graphs in a project.
// A lease expiry alone must never release ownership or start a second attempt:
// recovery first looks up the session for the stable attempt ID.
type Store interface {
	ListActiveGraphs(context.Context) ([]domain.ExecutionGraph, error)
	MarkReady(context.Context, string, string, time.Time) (bool, error)
	ClaimTask(context.Context, Claim) (bool, error)
	MarkRunning(context.Context, string, string, string, domain.SessionID, time.Time) (bool, error)
	MarkReview(context.Context, string, string, string, time.Time) (bool, error)
	ResolveReview(context.Context, string, string, string, ReviewDecision, time.Time) (bool, error)
	FailTask(context.Context, string, string, string, string, time.Time) (bool, error)
}

// WorkerState is an observed session outcome. Only WorkerDone enters REVIEW.
type WorkerState string

const (
	WorkerRunning WorkerState = "running"
	WorkerDone    WorkerState = "done"
	WorkerFailed  WorkerState = "failed"
	WorkerUnknown WorkerState = "unknown"
)

type WorkerObservation struct {
	State WorkerState
	Error string
}

// LaunchFailure is a definitive failed session launch observed during lookup.
// Ordinary lookup errors remain uncertain and must retain the ASSIGNED claim.
type LaunchFailure struct{ Reason string }

func (e LaunchFailure) Error() string { return e.Reason }

// Dispatcher starts a worker and reconciles it by the stable attempt key.
// Spawn returns when the session is started, without waiting for task work.
type Dispatcher interface {
	Spawn(context.Context, domain.ExecutionGraph, domain.ExecutionTask, Agent, string) (domain.SessionID, error)
	Lookup(context.Context, string) (domain.SessionID, bool, error)
	Inspect(context.Context, domain.SessionID) (WorkerObservation, error)
}

// Agent is a current, observed launch option. Slots must reflect availability;
// BudgetBlocked is true only when a configured budget forbids more dispatches.
type Agent struct {
	ID            string
	Provider      string
	Capabilities  []string
	Slots         int
	BudgetBlocked bool
}

type Capacity interface {
	Available(context.Context, domain.ProjectID) ([]Agent, error)
}

type Validator interface {
	Validate(context.Context, domain.ExecutionGraph, domain.ExecutionTask) (ReviewDecision, error)
}

type Config struct {
	Clock             func() time.Time
	NewAttemptID      func() string
	Logger            *slog.Logger
	MaxConcurrent     int
	MaxPerProvider    map[string]int
	MaxAttempts       int
	SpawnLease        time.Duration
	ReconcileInterval time.Duration
}

// Service maintains a READY frontier and launches every safe independent task
// in a pass. It uses a bounded wake channel and periodic reconciliation so
// missed events do not strand work.
type Service struct {
	store      Store
	dispatcher Dispatcher
	capacity   Capacity
	validator  Validator
	cfg        Config
	wake       chan struct{}
	tickMu     sync.Mutex
	workMu     sync.Mutex
	inflight   map[string]struct{}
	unresolved map[string]struct{}
	work       sync.WaitGroup
}

func New(store Store, dispatcher Dispatcher, capacity Capacity, validator Validator, cfg Config) (*Service, error) {
	if store == nil || dispatcher == nil || capacity == nil || validator == nil {
		return nil, errors.New("execution: store, dispatcher, capacity and validator are required")
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.NewAttemptID == nil {
		cfg.NewAttemptID = randomAttemptID
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.SpawnLease <= 0 {
		cfg.SpawnLease = 2 * time.Minute
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 15 * time.Second
	}
	return &Service{store: store, dispatcher: dispatcher, capacity: capacity, validator: validator,
		cfg: cfg, wake: make(chan struct{}, 1), inflight: make(map[string]struct{}), unresolved: make(map[string]struct{})}, nil
}

func randomAttemptID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Errorf("execution: create attempt id: %w", err))
	}
	return "execution-attempt-" + hex.EncodeToString(bytes[:])
}

// Wake coalesces change notifications. The durable state is always re-read;
// losing one wake only delays scheduling until the next periodic pass.
func (s *Service) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run reconciles immediately, then on change events, internal completions and
// a fallback cadence. The caller owns ctx for the daemon's full lifetime.
func (s *Service) Run(ctx context.Context, events <-chan struct{}) error {
	ticker := time.NewTicker(s.cfg.ReconcileInterval)
	defer ticker.Stop()
	if err := s.Reconcile(ctx); err != nil && ctx.Err() == nil {
		s.cfg.Logger.Warn("execution startup reconciliation failed; retrying", "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			s.work.Wait()
			return ctx.Err()
		case _, ok := <-events:
			if !ok {
				events = nil
			}
		case <-s.wake:
		case <-ticker.C:
		}
		if err := s.Reconcile(ctx); err != nil && ctx.Err() == nil {
			s.cfg.Logger.Warn("execution reconciliation failed; retrying", "err", err)
		}
	}
}

// Reconcile schedules before probing slow workers or validating completed
// ones, so an unrelated inspection cannot delay an already READY task.
func (s *Service) Reconcile(ctx context.Context) error {
	if err := s.Tick(ctx); err != nil {
		return err
	}
	graphs, err := s.store.ListActiveGraphs(ctx)
	if err != nil {
		return err
	}
	now := s.cfg.Clock().UTC()
	for _, graph := range graphs {
		for _, task := range graph.Tasks {
			if task.Assignment == nil {
				continue
			}
			switch task.State {
			case domain.TaskAssigned, domain.TaskRunning, domain.TaskReview:
				key := workKey(graph.ID, task.ID, task.Assignment.AttemptID)
				if !s.beginWork(key) {
					continue
				}
				s.work.Add(1)
				go func(g domain.ExecutionGraph, t domain.ExecutionTask) {
					defer s.work.Done()
					defer s.endWork(key)
					s.reconcileTask(ctx, g, t, now)
				}(graph, task)
			}
		}
	}
	return nil
}

// Tick advances eligible tasks and launches the whole safe frontier. It never
// waits for a spawn, worker execution or validation before considering peers.
func (s *Service) Tick(ctx context.Context) error {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	graphs, err := s.store.ListActiveGraphs(ctx)
	if err != nil {
		return err
	}
	now := s.cfg.Clock().UTC()
	changed := false
	for _, graph := range graphs {
		states := taskStates(graph)
		for _, task := range graph.Tasks {
			if (task.State != domain.TaskPending && !(task.State == domain.TaskFailed && retryableFailure(task, s.cfg.MaxAttempts))) || !dependenciesComplete(task, states) {
				continue
			}
			if ok, markErr := s.store.MarkReady(ctx, graph.ID, task.ID, now); markErr != nil {
				return markErr
			} else if ok {
				changed = true
			}
		}
	}
	if changed {
		graphs, err = s.store.ListActiveGraphs(ctx)
		if err != nil {
			return err
		}
	}
	agentsByProject := map[domain.ProjectID][]Agent{}
	agentsByID := map[string]Agent{}
	for _, graph := range graphs {
		if _, exists := agentsByProject[graph.ProjectID]; exists {
			continue
		}
		agents, getErr := s.capacity.Available(ctx, graph.ProjectID)
		if getErr != nil {
			return getErr
		}
		agentsByProject[graph.ProjectID] = agents
		for _, agent := range agents {
			agentsByID[agent.ID] = agent
		}
	}
	active, providerActive := activeCounts(graphs, agentsByID)
	reserved := map[string]int{}
	candidates := readyCandidates(graphs)
	for _, candidate := range candidates {
		if s.cfg.MaxConcurrent > 0 && active >= s.cfg.MaxConcurrent {
			break
		}
		agent, ok := chooseAgent(candidate.task, agentsByProject[candidate.graph.ProjectID], reserved, providerActive, s.cfg.MaxPerProvider)
		if !ok {
			continue
		}
		attemptID := s.cfg.NewAttemptID()
		if strings.TrimSpace(attemptID) == "" {
			return errors.New("execution: empty attempt id")
		}
		claim := Claim{GraphID: candidate.graph.ID, ContractFingerprint: TaskContractFingerprint(candidate.graph, candidate.task), TaskID: candidate.task.ID, AttemptID: attemptID,
			AgentID: agent.ID, Provider: agent.Provider, Resources: append([]string(nil), candidate.task.Resources...),
			At: now, LeaseExpiresAt: now.Add(s.cfg.SpawnLease), MaxConcurrent: s.cfg.MaxConcurrent,
			MaxPerProvider: s.cfg.MaxPerProvider[agent.Provider]}
		claimed, claimErr := s.store.ClaimTask(ctx, claim)
		if claimErr != nil {
			return claimErr
		}
		if !claimed {
			continue
		}
		active++
		providerActive[agent.Provider]++
		reserved[agent.ID]++
		key := workKey(candidate.graph.ID, candidate.task.ID, attemptID)
		if !s.beginWork(key) {
			return fmt.Errorf("execution: duplicate in-process claim %s", key)
		}
		s.work.Add(1)
		go func(g domain.ExecutionGraph, t domain.ExecutionTask, a Agent, id string) {
			defer s.work.Done()
			defer s.endWork(workKey(g.ID, t.ID, id))
			s.dispatch(ctx, g, t, a, id)
		}(candidate.graph, candidate.task, agent, attemptID)
	}
	return nil
}

func (s *Service) dispatch(ctx context.Context, graph domain.ExecutionGraph, task domain.ExecutionTask, agent Agent, attemptID string) {
	spawnCtx, stopSpawn := context.WithTimeout(ctx, s.cfg.SpawnLease)
	defer stopSpawn()
	sessionID, err := s.dispatcher.Spawn(spawnCtx, graph, task, agent, attemptID)
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err != nil {
		// A failed response may still have created a durable session. Adopting it
		// is required before any retry can be safe.
		if foundID, found, lookupErr := s.dispatcher.Lookup(writeCtx, attemptID); lookupErr == nil && found {
			_, _ = s.store.MarkRunning(writeCtx, graph.ID, task.ID, attemptID, foundID, s.cfg.Clock().UTC())
			s.Wake()
			return
		} else if lookupErr != nil {
			s.logUnresolved(graph.ID, task.ID, attemptID, lookupErr.Error())
			return
		}
		// Lookup(false) is a snapshot, not proof that a timed-out spawn can no
		// longer create a session. Retain the assignment and resource claims for
		// manual reconciliation instead of launching a duplicate attempt.
		s.logUnresolved(graph.ID, task.ID, attemptID, err.Error())
		return
	}
	_, _ = s.store.MarkRunning(writeCtx, graph.ID, task.ID, attemptID, sessionID, s.cfg.Clock().UTC())
	s.Wake()
}

func (s *Service) reconcileTask(ctx context.Context, graph domain.ExecutionGraph, task domain.ExecutionTask, now time.Time) {
	assignment := task.Assignment
	if assignment == nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	probeCtx, stopProbe := context.WithTimeout(ctx, 10*time.Second)
	defer stopProbe()
	switch task.State {
	case domain.TaskAssigned:
		id, found, err := s.dispatcher.Lookup(probeCtx, assignment.AttemptID)
		if err != nil {
			var launchFailure LaunchFailure
			if errors.As(err, &launchFailure) {
				_, _ = s.store.FailTask(writeCtx, graph.ID, task.ID, assignment.AttemptID, launchFailure.Error(), s.cfg.Clock().UTC())
				s.Wake()
			}
			return
		}
		if found {
			_, _ = s.store.MarkRunning(writeCtx, graph.ID, task.ID, assignment.AttemptID, id, s.cfg.Clock().UTC())
			s.Wake()
		} else if now.Sub(assignment.AssignedAt) >= s.cfg.SpawnLease {
			// An expired lease is a reason to investigate, never proof that a
			// launch did not commit after this lookup.
			s.logUnresolved(graph.ID, task.ID, assignment.AttemptID, "spawn lease expired without a committed session")
		}
	case domain.TaskRunning:
		if assignment.SessionID == nil {
			return
		}
		observation, err := s.dispatcher.Inspect(probeCtx, *assignment.SessionID)
		if err != nil {
			return // an inspection error is not proof the worker failed
		}
		switch observation.State {
		case WorkerDone:
			_, _ = s.store.MarkReview(writeCtx, graph.ID, task.ID, assignment.AttemptID, s.cfg.Clock().UTC())
			s.Wake()
		case WorkerFailed:
			_, _ = s.store.FailTask(writeCtx, graph.ID, task.ID, assignment.AttemptID, observation.Error, s.cfg.Clock().UTC())
			s.Wake()
		}
	case domain.TaskReview:
		decision, err := s.validator.Validate(probeCtx, graph, task)
		if err != nil {
			return // retain REVIEW so validation can be retried independently
		}
		if decision != ReviewAccept && decision != ReviewRevise && decision != ReviewReject {
			return
		}
		if decision == ReviewRevise && len(task.Attempts) >= s.cfg.MaxAttempts {
			decision = ReviewReject
		}
		_, _ = s.store.ResolveReview(writeCtx, graph.ID, task.ID, assignment.AttemptID, decision, s.cfg.Clock().UTC())
		s.Wake()
	}
}

func (s *Service) beginWork(key string) bool {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if _, exists := s.inflight[key]; exists {
		return false
	}
	s.inflight[key] = struct{}{}
	return true
}

func (s *Service) endWork(key string) {
	s.workMu.Lock()
	delete(s.inflight, key)
	s.workMu.Unlock()
}

func (s *Service) logUnresolved(graphID, taskID, attemptID, reason string) {
	key := workKey(graphID, taskID, attemptID)
	s.workMu.Lock()
	if _, logged := s.unresolved[key]; logged {
		s.workMu.Unlock()
		return
	}
	s.unresolved[key] = struct{}{}
	s.workMu.Unlock()
	s.cfg.Logger.Warn("execution launch unresolved; assignment and resources retained for manual reconciliation",
		"graph", graphID, "task", taskID, "attempt", attemptID, "reason", reason)
}

func workKey(graphID, taskID, attemptID string) string {
	return graphID + "/" + taskID + "/" + attemptID
}

// TaskContractFingerprint fences a claim against a plan edit made after the
// scheduler read its candidate. Runtime state and scheduling priority are not
// part of the worker brief; unrelated claims may advance graph revision without
// invalidating this task's contract.
func TaskContractFingerprint(graph domain.ExecutionGraph, task domain.ExecutionTask) string {
	task.State = ""
	task.Assignment = nil
	task.Attempts = nil
	task.Priority = 0
	body, _ := json.Marshal(struct {
		GraphObjective string               `json:"graphObjective"`
		Task           domain.ExecutionTask `json:"task"`
	}{GraphObjective: graph.Objective, Task: task})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func taskStates(graph domain.ExecutionGraph) map[string]domain.TaskState {
	states := make(map[string]domain.TaskState, len(graph.Tasks))
	for _, task := range graph.Tasks {
		states[task.ID] = task.State
	}
	return states
}

func dependenciesComplete(task domain.ExecutionTask, states map[string]domain.TaskState) bool {
	for _, id := range task.Dependencies {
		if states[id] != domain.TaskCompleted {
			return false
		}
	}
	return true
}

func retryableFailure(task domain.ExecutionTask, maxAttempts int) bool {
	if len(task.Attempts) == 0 || len(task.Attempts) >= maxAttempts {
		return false
	}
	return task.Attempts[len(task.Attempts)-1].Retryable
}

type candidate struct {
	graph   domain.ExecutionGraph
	task    domain.ExecutionTask
	unlocks int
}

func readyCandidates(graphs []domain.ExecutionGraph) []candidate {
	var candidates []candidate
	for _, graph := range graphs {
		states := taskStates(graph)
		for _, task := range graph.Tasks {
			if task.State == domain.TaskReady && dependenciesComplete(task, states) {
				candidates = append(candidates, candidate{graph: graph, task: task, unlocks: downstreamCount(graph, task.ID)})
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].task.Priority != candidates[j].task.Priority {
			return candidates[i].task.Priority > candidates[j].task.Priority
		}
		if candidates[i].unlocks != candidates[j].unlocks {
			return candidates[i].unlocks > candidates[j].unlocks
		}
		if candidates[i].graph.ID != candidates[j].graph.ID {
			return candidates[i].graph.ID < candidates[j].graph.ID
		}
		return candidates[i].task.ID < candidates[j].task.ID
	})
	return candidates
}

func downstreamCount(graph domain.ExecutionGraph, root string) int {
	seen := map[string]struct{}{}
	var visit func(string)
	visit = func(id string) {
		for _, task := range graph.Tasks {
			for _, dep := range task.Dependencies {
				if dep == id {
					if _, exists := seen[task.ID]; !exists {
						seen[task.ID] = struct{}{}
						visit(task.ID)
					}
				}
			}
		}
	}
	visit(root)
	return len(seen)
}

func activeCounts(graphs []domain.ExecutionGraph, agents map[string]Agent) (int, map[string]int) {
	providers := map[string]int{}
	active := 0
	for _, graph := range graphs {
		for _, task := range graph.Tasks {
			if task.State != domain.TaskAssigned && task.State != domain.TaskRunning {
				continue
			}
			active++
			if task.Assignment != nil {
				provider := task.Assignment.Provider
				if provider == "" {
					provider = agents[task.Assignment.AgentID].Provider
				}
				providers[provider]++
			}
		}
	}
	return active, providers
}

func chooseAgent(task domain.ExecutionTask, agents []Agent, reserved map[string]int, providerActive map[string]int, providerLimits map[string]int) (Agent, bool) {
	order := make(map[string]int)
	if task.PreferredAgent != "" {
		order[task.PreferredAgent] = 0
	}
	for i, id := range task.FallbackAgents {
		if _, exists := order[id]; !exists {
			order[id] = i + 1
		}
	}
	choices := append([]Agent(nil), agents...)
	sort.SliceStable(choices, func(i, j int) bool {
		ai, listedI := order[choices[i].ID]
		aj, listedJ := order[choices[j].ID]
		if listedI != listedJ {
			return listedI
		}
		if listedI && ai != aj {
			return ai < aj
		}
		return choices[i].ID < choices[j].ID
	})
	for _, agent := range choices {
		if agent.ID == "" || agent.Slots-reserved[agent.ID] <= 0 || agent.BudgetBlocked || !hasCapabilities(agent.Capabilities, task.RecommendedCapabilities) {
			continue
		}
		if limit := providerLimits[agent.Provider]; limit > 0 && providerActive[agent.Provider] >= limit {
			continue
		}
		return agent, true
	}
	return Agent{}, false
}

func hasCapabilities(have, need []string) bool {
	available := make(map[string]struct{}, len(have))
	for _, capability := range have {
		available[capability] = struct{}{}
	}
	for _, capability := range need {
		if _, ok := available[capability]; !ok {
			return false
		}
	}
	return true
}
