package domain

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// ExecutionGraphSchemaVersion is the wire version of an approved plan and its
// evolving execution facts. Graph and task IDs remain stable across revisions.
const ExecutionGraphSchemaVersion = 1

// TaskState is the durable lifecycle of a project task. Unlike SessionStatus,
// this is an execution fact, not a derived board display status.
type TaskState string

const (
	TaskPending   TaskState = "pending"
	TaskReady     TaskState = "ready"
	TaskAssigned  TaskState = "assigned"
	TaskRunning   TaskState = "running"
	TaskBlocked   TaskState = "blocked"
	TaskReview    TaskState = "review"
	TaskCompleted TaskState = "completed"
	TaskFailed    TaskState = "failed"
	TaskCancelled TaskState = "cancelled"
)

// ExecutionGraph preserves the approved plan while task states and attempts
// change. Revision starts at one and advances on each persisted graph edit.
type ExecutionGraph struct {
	SchemaVersion int              `json:"schemaVersion"`
	ID            string           `json:"id"`
	Revision      int64            `json:"revision"`
	ProjectID     ProjectID        `json:"projectId"`
	Objective     string           `json:"objective"`
	Slices        []ExecutionSlice `json:"slices"`
	Tasks         []ExecutionTask  `json:"tasks"`
}

// PreparedExecutionGraphReceipt identifies inert stored full-graph bytes. Its
// digest is SHA-256 of AO's JSON encoding of the entire ExecutionGraph. It is
// distinct from the A1 graphContractDigest, which excludes runtime facts.
type PreparedExecutionGraphReceipt struct {
	PrepareID        string    `json:"prepareId"`
	GraphID          string    `json:"graphId"`
	ProjectID        ProjectID `json:"projectId"`
	GraphBytesSHA256 string    `json:"graphBytesSha256"`
	Status           string    `json:"status"`
	CreatedAt        time.Time `json:"createdAt"`
}

// ExecutionSlice groups independently useful work. Task dependencies, rather
// than wave membership or slice order, control execution eligibility.
type ExecutionSlice struct {
	ID                 string   `json:"id"`
	Objective          string   `json:"objective"`
	Scope              string   `json:"scope"`
	Deliverables       []string `json:"deliverables"`
	AcceptanceCriteria []string `json:"acceptanceCriteria"`
}

// ExecutionTask carries the approved task contract and current execution facts.
// Dependencies are hard prerequisites. SoftDependencies are advisory and never
// prevent a task from becoming ready. Resources name exclusive edit ownership;
// the scheduler must lease them atomically before assigning work.
type ExecutionTask struct {
	ID                      string             `json:"id"`
	SliceID                 string             `json:"sliceId"`
	Objective               string             `json:"objective"`
	Scope                   string             `json:"scope"`
	Deliverables            []string           `json:"deliverables"`
	AcceptanceCriteria      []string           `json:"acceptanceCriteria"`
	Dependencies            []string           `json:"dependencies"`
	SoftDependencies        []string           `json:"softDependencies,omitempty"`
	Priority                int                `json:"priority"`
	RecommendedCapabilities []string           `json:"recommendedCapabilities"`
	PreferredAgent          string             `json:"preferredAgent,omitempty"`
	FallbackAgents          []string           `json:"fallbackAgents"`
	Resources               []string           `json:"resources"`
	IntegrationRequirements []string           `json:"integrationRequirements"`
	ValidationProcedures    []string           `json:"validationProcedures"`
	Complexity              string             `json:"complexity"`
	State                   TaskState          `json:"state"`
	Assignment              *TaskAssignment    `json:"assignment,omitempty"`
	Attempts                []TaskAttempt      `json:"attempts"`
	Reviews                 []TaskReviewRecord `json:"reviews,omitempty"`
}

// TaskReviewRecord is one immutable coordinator decision about a worker attempt.
// Source and validation fields are references and short summaries, not logs.
type TaskReviewRecord struct {
	AttemptID         string    `json:"attemptId"`
	Decision          string    `json:"decision"`
	ReviewedAt        time.Time `json:"reviewedAt"`
	SourceHead        string    `json:"sourceHead,omitempty"`
	PRURL             string    `json:"prUrl,omitempty"`
	ValidationSummary string    `json:"validationSummary,omitempty"`
	EvidenceRefs      []string  `json:"evidenceRefs,omitempty"`
}

// TaskAssignment identifies the worker currently responsible for a task.
// SessionID may be absent while an asynchronous spawn is in progress.
type TaskAssignment struct {
	AgentID    string     `json:"agentId"`
	Provider   string     `json:"provider,omitempty"`
	AttemptID  string     `json:"attemptId"`
	SessionID  *SessionID `json:"sessionId,omitempty"`
	AssignedAt time.Time  `json:"assignedAt"`
}

// TaskAttempt records one dispatch outcome without erasing prior attempts.
type TaskAttempt struct {
	ID         string     `json:"id"`
	AgentID    string     `json:"agentId"`
	SessionID  *SessionID `json:"sessionId,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Retryable  bool       `json:"retryable,omitempty"`
	Error      string     `json:"error,omitempty"`
}

// CanTransitionTaskState checks a single lifecycle edge. Completed and
// cancelled work is immutable; retries of failed work return to ready.
func CanTransitionTaskState(from, to TaskState) bool {
	switch from {
	case TaskPending:
		return to == TaskReady || to == TaskBlocked || to == TaskCancelled
	case TaskReady:
		return to == TaskAssigned || to == TaskBlocked || to == TaskCancelled
	case TaskAssigned:
		return to == TaskRunning || to == TaskReady || to == TaskBlocked || to == TaskFailed || to == TaskCancelled
	case TaskRunning:
		return to == TaskReview || to == TaskBlocked || to == TaskFailed || to == TaskCancelled
	case TaskBlocked:
		return to == TaskReady || to == TaskRunning || to == TaskFailed || to == TaskCancelled
	case TaskReview:
		return to == TaskCompleted || to == TaskReady || to == TaskRunning || to == TaskBlocked || to == TaskFailed || to == TaskCancelled
	case TaskFailed:
		return to == TaskReady || to == TaskCancelled
	default:
		return false
	}
}

// ValidateExecutionGraph rejects malformed plans, cyclic hard dependencies,
// invalid execution facts, and simultaneously assigned exclusive resources.
// It does not decide agent availability or mutate task readiness.
func ValidateExecutionGraph(graph ExecutionGraph) error {
	if graph.SchemaVersion != ExecutionGraphSchemaVersion {
		return fmt.Errorf("execution graph: unsupported schema version %d", graph.SchemaVersion)
	}
	if err := required("execution graph id", graph.ID); err != nil {
		return err
	}
	if graph.Revision < 1 {
		return fmt.Errorf("execution graph %q: revision must be positive", graph.ID)
	}
	if err := required("execution graph projectId", string(graph.ProjectID)); err != nil {
		return err
	}
	if err := required("execution graph objective", graph.Objective); err != nil {
		return err
	}
	slices := make(map[string]struct{}, len(graph.Slices))
	for _, slice := range graph.Slices {
		if err := required("slice id", slice.ID); err != nil {
			return err
		}
		if _, duplicate := slices[slice.ID]; duplicate {
			return fmt.Errorf("execution graph: duplicate slice id %q", slice.ID)
		}
		slices[slice.ID] = struct{}{}
		if err := required("slice "+slice.ID+" objective", slice.Objective); err != nil {
			return err
		}
		if err := required("slice "+slice.ID+" scope", slice.Scope); err != nil {
			return err
		}
		if err := requiredItems("slice "+slice.ID+" deliverables", slice.Deliverables); err != nil {
			return err
		}
		if err := requiredItems("slice "+slice.ID+" acceptanceCriteria", slice.AcceptanceCriteria); err != nil {
			return err
		}
	}
	tasks := make(map[string]ExecutionTask, len(graph.Tasks))
	for _, task := range graph.Tasks {
		if err := required("task id", task.ID); err != nil {
			return err
		}
		if _, duplicate := tasks[task.ID]; duplicate {
			return fmt.Errorf("execution graph: duplicate task id %q", task.ID)
		}
		tasks[task.ID] = task
		if _, exists := slices[task.SliceID]; !exists {
			return fmt.Errorf("task %q: unknown slice %q", task.ID, task.SliceID)
		}
		if err := required("task "+task.ID+" objective", task.Objective); err != nil {
			return err
		}
		if err := required("task "+task.ID+" scope", task.Scope); err != nil {
			return err
		}
		if err := requiredItems("task "+task.ID+" deliverables", task.Deliverables); err != nil {
			return err
		}
		if err := requiredItems("task "+task.ID+" acceptanceCriteria", task.AcceptanceCriteria); err != nil {
			return err
		}
		if err := requiredItems("task "+task.ID+" validationProcedures", task.ValidationProcedures); err != nil {
			return err
		}
		if err := required("task "+task.ID+" complexity", task.Complexity); err != nil {
			return err
		}
		if !validTaskState(task.State) {
			return fmt.Errorf("task %q: invalid state %q", task.ID, task.State)
		}
		if task.State == TaskAssigned || task.State == TaskRunning {
			if task.Assignment == nil || strings.TrimSpace(task.Assignment.AgentID) == "" || strings.TrimSpace(task.Assignment.AttemptID) == "" {
				return fmt.Errorf("task %q: active task requires agent and attempt assignment", task.ID)
			}
			if err := validateActiveAttempt(task); err != nil {
				return err
			}
		}
		if err := uniqueItems("task "+task.ID+" resources", task.Resources); err != nil {
			return err
		}
		if err := uniqueItems("task "+task.ID+" attempts", attemptIDs(task.Attempts)); err != nil {
			return err
		}
		if err := validateTaskReviews(task); err != nil {
			return err
		}
	}
	for _, task := range graph.Tasks {
		seen := make(map[string]struct{})
		for _, dep := range task.Dependencies {
			if err := validateDependency(task.ID, dep, tasks, seen); err != nil {
				return err
			}
			if requiresCompletedDependencies(task.State) && tasks[dep].State != TaskCompleted {
				return fmt.Errorf("task %q: dependency %q is not completed", task.ID, dep)
			}
		}
		for _, dep := range task.SoftDependencies {
			if err := validateDependency(task.ID, dep, tasks, seen); err != nil {
				return err
			}
		}
	}
	if err := validateHardDependencyCycles(tasks); err != nil {
		return err
	}
	activeResources := make(map[string]string)
	for _, task := range graph.Tasks {
		if task.State != TaskAssigned && task.State != TaskRunning {
			continue
		}
		for _, resource := range task.Resources {
			if owner, exists := activeResources[resource]; exists {
				return fmt.Errorf("tasks %q and %q both own exclusive resource %q", owner, task.ID, resource)
			}
			activeResources[resource] = task.ID
		}
	}
	return nil
}

func validateActiveAttempt(task ExecutionTask) error {
	for _, attempt := range task.Attempts {
		if attempt.ID != task.Assignment.AttemptID {
			continue
		}
		if attempt.AgentID != task.Assignment.AgentID {
			return fmt.Errorf("task %q: active assignment agent differs from recorded attempt", task.ID)
		}
		if attempt.FinishedAt != nil {
			return fmt.Errorf("task %q: active assignment references a finished attempt", task.ID)
		}
		if task.Assignment.SessionID != nil && attempt.SessionID != nil && *task.Assignment.SessionID != *attempt.SessionID {
			return fmt.Errorf("task %q: active assignment session differs from recorded attempt", task.ID)
		}
		return nil
	}
	return fmt.Errorf("task %q: active assignment must reference a recorded attempt", task.ID)
}

// ValidateExecutionGraphTransition checks continuity between two durable
// revisions. Replanning may add tasks and edit unfinished plan fields, but it
// cannot erase earlier task IDs, attempts, or accepted completed work.
func ValidateExecutionGraphTransition(previous, next ExecutionGraph) error {
	if err := ValidateExecutionGraph(previous); err != nil {
		return fmt.Errorf("previous graph: %w", err)
	}
	if err := ValidateExecutionGraph(next); err != nil {
		return fmt.Errorf("next graph: %w", err)
	}
	if previous.ID != next.ID || previous.ProjectID != next.ProjectID || previous.SchemaVersion != next.SchemaVersion {
		return fmt.Errorf("execution graph identity changed between revisions")
	}
	if next.Revision != previous.Revision+1 {
		return fmt.Errorf("execution graph %q: revision must advance by one", previous.ID)
	}
	nextSlices := make(map[string]struct{}, len(next.Slices))
	for _, slice := range next.Slices {
		nextSlices[slice.ID] = struct{}{}
	}
	for _, slice := range previous.Slices {
		if _, exists := nextSlices[slice.ID]; !exists {
			return fmt.Errorf("execution graph %q: slice %q was removed", previous.ID, slice.ID)
		}
	}
	nextTasks := make(map[string]ExecutionTask, len(next.Tasks))
	for _, task := range next.Tasks {
		nextTasks[task.ID] = task
	}
	for _, oldTask := range previous.Tasks {
		newTask, exists := nextTasks[oldTask.ID]
		if !exists {
			return fmt.Errorf("execution graph %q: task %q was removed; cancel it instead", previous.ID, oldTask.ID)
		}
		if oldTask.SliceID != newTask.SliceID {
			return fmt.Errorf("task %q: slice identity changed", oldTask.ID)
		}
		if oldTask.State != newTask.State && !CanTransitionTaskState(oldTask.State, newTask.State) {
			return fmt.Errorf("task %q: invalid transition %s -> %s", oldTask.ID, oldTask.State, newTask.State)
		}
		if oldTask.State == TaskCompleted && !reflect.DeepEqual(oldTask, newTask) {
			return fmt.Errorf("task %q: accepted completed work changed", oldTask.ID)
		}
		if err := validateAttemptContinuity(oldTask.Attempts, newTask.Attempts); err != nil {
			return fmt.Errorf("task %q: %w", oldTask.ID, err)
		}
		if len(newTask.Reviews) < len(oldTask.Reviews) {
			return fmt.Errorf("task %q: review history changed", oldTask.ID)
		}
		for i := range oldTask.Reviews {
			if !reflect.DeepEqual(oldTask.Reviews[i], newTask.Reviews[i]) {
				return fmt.Errorf("task %q: review history changed", oldTask.ID)
			}
		}
		if (oldTask.State == TaskAssigned || oldTask.State == TaskRunning) &&
			(newTask.State == TaskAssigned || newTask.State == TaskRunning) {
			if !reflect.DeepEqual(oldTask.Resources, newTask.Resources) {
				return fmt.Errorf("task %q: active resource ownership changed", oldTask.ID)
			}
			if oldTask.Assignment == nil || newTask.Assignment == nil ||
				oldTask.Assignment.AgentID != newTask.Assignment.AgentID ||
				oldTask.Assignment.AttemptID != newTask.Assignment.AttemptID ||
				(oldTask.Assignment.Provider != "" && oldTask.Assignment.Provider != newTask.Assignment.Provider) {
				return fmt.Errorf("task %q: active assignment changed", oldTask.ID)
			}
		}
	}
	return nil
}

func validateTaskReviews(task ExecutionTask) error {
	attempts := make(map[string]TaskAttempt, len(task.Attempts))
	for _, attempt := range task.Attempts {
		attempts[attempt.ID] = attempt
	}
	seen := make(map[string]struct{}, len(task.Reviews))
	for _, review := range task.Reviews {
		if err := required("task "+task.ID+" review attemptId", review.AttemptID); err != nil {
			return err
		}
		attempt, exists := attempts[review.AttemptID]
		if !exists || attempt.FinishedAt == nil {
			return fmt.Errorf("task %q: review must reference a finished attempt", task.ID)
		}
		if _, duplicate := seen[review.AttemptID]; duplicate {
			return fmt.Errorf("task %q: duplicate review for attempt %q", task.ID, review.AttemptID)
		}
		seen[review.AttemptID] = struct{}{}
		if review.Decision != "accept" && review.Decision != "revise" && review.Decision != "reject" {
			return fmt.Errorf("task %q: invalid review decision %q", task.ID, review.Decision)
		}
		if review.ReviewedAt.IsZero() {
			return fmt.Errorf("task %q: review time is required", task.ID)
		}
		if err := ValidateTaskReviewEvidence(review); err != nil {
			return fmt.Errorf("task %q: %w", task.ID, err)
		}
	}
	return nil
}

// ValidateTaskReviewEvidence bounds coordinator-supplied references so graph
// snapshots remain small and do not become a store for test transcripts.
func ValidateTaskReviewEvidence(review TaskReviewRecord) error {
	if len(review.SourceHead) > 128 || len(review.PRURL) > 2048 || len(review.ValidationSummary) > 2000 || len(review.EvidenceRefs) > 20 {
		return fmt.Errorf("review evidence exceeds size limit")
	}
	for _, value := range []string{review.SourceHead, review.PRURL, review.ValidationSummary} {
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("review evidence values must be trimmed")
		}
	}
	if review.SourceHead != "" {
		if len(review.SourceHead) != 40 && len(review.SourceHead) != 64 {
			return fmt.Errorf("review sourceHead must be a full Git object ID")
		}
		if _, err := hex.DecodeString(review.SourceHead); err != nil {
			return fmt.Errorf("review sourceHead must be hexadecimal")
		}
	}
	if review.PRURL != "" {
		parsed, err := url.Parse(review.PRURL)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("review prUrl must be an HTTPS URL without credentials")
		}
	}
	for _, ref := range review.EvidenceRefs {
		if len(ref) > 2048 || strings.TrimSpace(ref) == "" || ref != strings.TrimSpace(ref) {
			return fmt.Errorf("review evidence reference must be nonempty, trimmed, and at most 2048 bytes")
		}
	}
	return nil
}

func validateAttemptContinuity(previous, next []TaskAttempt) error {
	if len(next) < len(previous) {
		return fmt.Errorf("attempt history changed: prior attempt removed")
	}
	for i, oldAttempt := range previous {
		newAttempt := next[i]
		if oldAttempt.ID != newAttempt.ID || oldAttempt.AgentID != newAttempt.AgentID {
			return fmt.Errorf("attempt history changed: prior attempt identity changed")
		}
		if oldAttempt.SessionID != nil &&
			(newAttempt.SessionID == nil || *oldAttempt.SessionID != *newAttempt.SessionID) {
			return fmt.Errorf("attempt history changed: prior session changed")
		}
		if !oldAttempt.StartedAt.IsZero() && !oldAttempt.StartedAt.Equal(newAttempt.StartedAt) {
			return fmt.Errorf("attempt history changed: prior start changed")
		}
		if oldAttempt.FinishedAt != nil {
			if !reflect.DeepEqual(oldAttempt, newAttempt) {
				return fmt.Errorf("attempt history changed: finished attempt changed")
			}
		} else if i < len(previous)-1 || (len(next) > len(previous) && newAttempt.FinishedAt == nil) {
			return fmt.Errorf("attempt history changed: earlier attempt is unfinished")
		}
	}
	return nil
}

func required(label, value string) error {
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be nonempty and trimmed", label)
	}
	return nil
}

func requiredItems(label string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("%s must contain at least one item", label)
	}
	return uniqueItems(label, values)
}

func uniqueItems(label string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := required(label+" item", value); err != nil {
			return err
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s contains duplicate %q", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func attemptIDs(attempts []TaskAttempt) []string {
	ids := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		ids = append(ids, attempt.ID)
	}
	return ids
}

func validTaskState(state TaskState) bool {
	switch state {
	case TaskPending, TaskReady, TaskAssigned, TaskRunning, TaskBlocked, TaskReview, TaskCompleted, TaskFailed, TaskCancelled:
		return true
	default:
		return false
	}
}

func requiresCompletedDependencies(state TaskState) bool {
	switch state {
	case TaskReady, TaskAssigned, TaskRunning, TaskReview, TaskCompleted:
		return true
	default:
		return false
	}
}

func validateDependency(taskID, dependencyID string, tasks map[string]ExecutionTask, seen map[string]struct{}) error {
	if err := required("task "+taskID+" dependency", dependencyID); err != nil {
		return err
	}
	if dependencyID == taskID {
		return fmt.Errorf("task %q: self dependency", taskID)
	}
	if _, exists := tasks[dependencyID]; !exists {
		return fmt.Errorf("task %q: unknown dependency %q", taskID, dependencyID)
	}
	if _, duplicate := seen[dependencyID]; duplicate {
		return fmt.Errorf("task %q: duplicate dependency %q", taskID, dependencyID)
	}
	seen[dependencyID] = struct{}{}
	return nil
}

func validateHardDependencyCycles(tasks map[string]ExecutionTask) error {
	visited := make(map[string]uint8, len(tasks))
	var visit func(string) error
	visit = func(id string) error {
		if visited[id] == 1 {
			return fmt.Errorf("execution graph: hard dependency cycle at task %q", id)
		}
		if visited[id] == 2 {
			return nil
		}
		visited[id] = 1
		for _, dep := range tasks[id].Dependencies {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visited[id] = 2
		return nil
	}
	for id := range tasks {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}
