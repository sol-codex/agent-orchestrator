package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ execution.Store = (*Store)(nil)

// SaveExecutionGraph imports an approved plan or updates its next revision.
// Runtime facts belong to the scheduler transitions below, so plan edits cannot
// silently manufacture assignments, worker outcomes, or acceptance.
func (s *Store) SaveExecutionGraph(ctx context.Context, graph domain.ExecutionGraph) error {
	if err := domain.ValidateExecutionGraph(graph); err != nil {
		return err
	}
	if err := execution.ValidateSupportedAgentContract(graph); err != nil {
		return err
	}
	for _, task := range graph.Tasks {
		seen := make(map[string]struct{}, len(task.Resources))
		for _, resource := range task.Resources {
			key := canonicalResource(resource)
			if _, exists := seen[key]; exists {
				return fmt.Errorf("task %q: resource aliases collide at %q", task.ID, key)
			}
			seen[key] = struct{}{}
		}
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.writeMu.Unlock()
	return s.inTxDB(ctx, "save execution graph", func(_ *gen.Queries, db gen.DBTX) error {
		var preparedID string
		err := db.QueryRowContext(ctx, `SELECT id FROM prepared_execution_graphs WHERE id=?`, graph.ID).Scan(&preparedID)
		if err == nil {
			return execution.ErrGraphConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		old, found, err := readExecutionGraph(ctx, db, graph.ID)
		if err != nil {
			return err
		}
		if !found {
			if graph.Revision != 1 {
				return execution.ErrGraphConflict
			}
			var projectID string
			if err := db.QueryRowContext(ctx, `SELECT id FROM projects WHERE id = ?`, string(graph.ProjectID)).Scan(&projectID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return execution.ErrGraphProjectNotFound
				}
				return err
			}
			for _, task := range graph.Tasks {
				if task.Assignment != nil || len(task.Attempts) != 0 || len(task.Reviews) != 0 || runtimeState(task.State) {
					return fmt.Errorf("%w: graph %q imported task %q", execution.ErrGraphRuntimeFacts, graph.ID, task.ID)
				}
			}
			body, err := json.Marshal(graph)
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			_, err = db.ExecContext(ctx, `INSERT INTO execution_graphs(id,project_id,revision,graph_json,created_at,updated_at) VALUES(?,?,?,?,?,?)`, graph.ID, string(graph.ProjectID), graph.Revision, string(body), now, now)
			return err
		}
		if graph.Revision != old.Revision+1 {
			return execution.ErrGraphConflict
		}
		if err := domain.ValidateExecutionGraphTransition(old, graph); err != nil {
			return err
		}
		if old.Objective != graph.Objective {
			for _, task := range old.Tasks {
				if task.State == domain.TaskAssigned || task.State == domain.TaskRunning || task.State == domain.TaskReview {
					return fmt.Errorf("%w: graph %q objective is in use by active task %q", execution.ErrGraphRuntimeFacts, graph.ID, task.ID)
				}
			}
		}
		oldTasks := make(map[string]domain.ExecutionTask, len(old.Tasks))
		for _, task := range old.Tasks {
			oldTasks[task.ID] = task
		}
		for _, task := range graph.Tasks {
			if prior, ok := oldTasks[task.ID]; ok {
				if (prior.State == domain.TaskAssigned || prior.State == domain.TaskRunning || prior.State == domain.TaskReview) && !reflect.DeepEqual(prior, task) {
					return fmt.Errorf("%w: graph %q active task %q contract is frozen", execution.ErrGraphRuntimeFacts, graph.ID, task.ID)
				}
				if prior.State != task.State || !reflect.DeepEqual(prior.Assignment, task.Assignment) || !reflect.DeepEqual(prior.Attempts, task.Attempts) || !reflect.DeepEqual(prior.Reviews, task.Reviews) {
					return fmt.Errorf("%w: graph %q task %q", execution.ErrGraphRuntimeFacts, graph.ID, task.ID)
				}
				if prior.Assignment != nil && !reflect.DeepEqual(prior.Resources, task.Resources) {
					return fmt.Errorf("%w: graph %q task %q claimed resources cannot change", execution.ErrGraphRuntimeFacts, graph.ID, task.ID)
				}
			} else if task.Assignment != nil || len(task.Attempts) != 0 || len(task.Reviews) != 0 || runtimeState(task.State) {
				return fmt.Errorf("%w: graph %q new task %q", execution.ErrGraphRuntimeFacts, graph.ID, task.ID)
			}
		}
		return writeExecutionGraph(ctx, db, old, graph, time.Now().UTC())
	})
}

// PrepareExecutionGraph stores a validated plan outside the scheduler's active
// graph table. Identical canonical graph bytes are an idempotent retry; any
// changed graph under the same ID conflicts. This method never emits a wake.
func (s *Store) PrepareExecutionGraph(ctx context.Context, prepareID string, graph domain.ExecutionGraph) (domain.PreparedExecutionGraphReceipt, bool, error) {
	var receipt domain.PreparedExecutionGraphReceipt
	if err := validatePrepareID(prepareID); err != nil {
		return receipt, false, err
	}
	if err := domain.ValidateExecutionGraph(graph); err != nil {
		return receipt, false, err
	}
	if err := execution.ValidateSupportedAgentContract(graph); err != nil {
		return receipt, false, err
	}
	if graph.Revision != 1 {
		return receipt, false, execution.ErrGraphConflict
	}
	for _, task := range graph.Tasks {
		if task.Assignment != nil || len(task.Attempts) != 0 || len(task.Reviews) != 0 || runtimeState(task.State) {
			return receipt, false, execution.ErrGraphRuntimeFacts
		}
		seen := make(map[string]struct{}, len(task.Resources))
		for _, resource := range task.Resources {
			key := canonicalResource(resource)
			if _, exists := seen[key]; exists {
				return receipt, false, fmt.Errorf("task %q: resource aliases collide at %q", task.ID, key)
			}
			seen[key] = struct{}{}
		}
	}
	body, err := json.Marshal(graph)
	if err != nil {
		return receipt, false, err
	}
	digestBytes := sha256.Sum256(body)
	digest := hex.EncodeToString(digestBytes[:])
	if err := s.writeMu.LockContext(ctx); err != nil {
		return receipt, false, err
	}
	defer s.writeMu.Unlock()
	created := false
	err = s.inTxDB(ctx, "prepare execution graph", func(_ *gen.Queries, db gen.DBTX) error {
		var activeID string
		err := db.QueryRowContext(ctx, `SELECT id FROM execution_graphs WHERE id=?`, graph.ID).Scan(&activeID)
		if err == nil {
			return execution.ErrGraphConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var priorID, priorGraphID, priorProjectID, priorDigest, priorReceiptDigest, priorBody string
		var priorCreated time.Time
		var matchingRows int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM prepared_execution_graphs WHERE id=? OR prepare_id=?`, graph.ID, prepareID).Scan(&matchingRows); err != nil {
			return err
		}
		if matchingRows > 1 {
			return execution.ErrGraphConflict
		}
		err = db.QueryRowContext(ctx, `SELECT prepare_id,id,project_id,graph_bytes_sha256,receipt_identity_sha256,graph_json,created_at FROM prepared_execution_graphs WHERE id=? OR prepare_id=?`, graph.ID, prepareID).Scan(&priorID, &priorGraphID, &priorProjectID, &priorDigest, &priorReceiptDigest, &priorBody, &priorCreated)
		if err == nil {
			priorReceipt, err := verifiedPreparedReceipt(priorID, priorGraphID, priorProjectID, priorDigest, priorReceiptDigest, priorBody, priorCreated)
			if err != nil {
				return err
			}
			if priorID == prepareID && priorGraphID == graph.ID && priorProjectID == string(graph.ProjectID) && priorDigest == digest && priorBody == string(body) {
				receipt = priorReceipt
				return nil
			}
			return execution.ErrGraphConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var projectID string
		if err := db.QueryRowContext(ctx, `SELECT id FROM projects WHERE id=?`, string(graph.ProjectID)).Scan(&projectID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return execution.ErrGraphProjectNotFound
			}
			return err
		}
		now := time.Now().UTC()
		receiptDigest, err := preparedReceiptIdentityDigest(prepareID, graph.ID, string(graph.ProjectID), digest, now)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO prepared_execution_graphs(id,project_id,graph_json,created_at,prepare_id,graph_bytes_sha256,receipt_identity_sha256) VALUES(?,?,?,?,?,?,?)`, graph.ID, string(graph.ProjectID), string(body), now, prepareID, digest, receiptDigest); err != nil {
			return err
		}
		receipt = domain.PreparedExecutionGraphReceipt{PrepareID: prepareID, GraphID: graph.ID, ProjectID: graph.ProjectID, GraphBytesSHA256: digest, ReceiptIdentitySHA256: receiptDigest, Status: "prepared", CreatedAt: now}
		created = true
		return nil
	})
	return receipt, created, err
}

func (s *Store) GetPreparedExecutionGraph(ctx context.Context, id string, byGraphID bool) (domain.PreparedExecutionGraphReceipt, bool, error) {
	var receipt domain.PreparedExecutionGraphReceipt
	var projectID, graphJSON, receiptDigest string
	var column = "prepare_id"
	if byGraphID {
		column = "id"
	}
	err := s.readDB.QueryRowContext(ctx, `SELECT prepare_id,id,project_id,graph_bytes_sha256,receipt_identity_sha256,graph_json,created_at FROM prepared_execution_graphs WHERE `+column+`=?`, id).Scan(&receipt.PrepareID, &receipt.GraphID, &projectID, &receipt.GraphBytesSHA256, &receiptDigest, &graphJSON, &receipt.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return receipt, false, nil
	}
	if err != nil {
		return receipt, false, err
	}
	verified, err := verifiedPreparedReceipt(receipt.PrepareID, receipt.GraphID, projectID, receipt.GraphBytesSHA256, receiptDigest, graphJSON, receipt.CreatedAt)
	if err != nil {
		return receipt, false, err
	}
	return verified, true, nil
}

func validatePrepareID(id string) error {
	if id == "" || len(id) > 128 || strings.TrimSpace(id) != id || strings.ContainsAny(id, "/?#") {
		return fmt.Errorf("prepare ID must be a trimmed path-safe value of at most 128 bytes")
	}
	return nil
}

// verifiedPreparedReceipt refuses a positive receipt if persisted bytes no
// longer match their full-graph digest or the row's graph/project identity.
func verifiedPreparedReceipt(prepareID, graphID, projectID, digest, receiptDigest, graphJSON string, createdAt time.Time) (domain.PreparedExecutionGraphReceipt, error) {
	var receipt domain.PreparedExecutionGraphReceipt
	if err := validatePrepareID(prepareID); err != nil {
		return receipt, fmt.Errorf("prepared graph %q has invalid prepare ID: %w", graphID, err)
	}
	expectedReceiptDigest, err := preparedReceiptIdentityDigest(prepareID, graphID, projectID, digest, createdAt)
	if err != nil || receiptDigest != expectedReceiptDigest {
		return receipt, fmt.Errorf("prepared graph %q failed receipt identity check", graphID)
	}
	var graph domain.ExecutionGraph
	if err := json.Unmarshal([]byte(graphJSON), &graph); err != nil {
		return receipt, fmt.Errorf("prepared graph %q corrupt JSON: %w", graphID, err)
	}
	canonical, err := json.Marshal(graph)
	if err != nil {
		return receipt, fmt.Errorf("prepared graph %q cannot encode: %w", graphID, err)
	}
	hash := sha256.Sum256(canonical)
	if string(canonical) != graphJSON || hex.EncodeToString(hash[:]) != digest || graph.ID != graphID || string(graph.ProjectID) != projectID {
		return receipt, fmt.Errorf("prepared graph %q failed stored-byte integrity check", graphID)
	}
	return domain.PreparedExecutionGraphReceipt{PrepareID: prepareID, GraphID: graphID, ProjectID: domain.ProjectID(projectID), GraphBytesSHA256: digest, ReceiptIdentitySHA256: receiptDigest, Status: "prepared", CreatedAt: createdAt}, nil
}

func preparedReceiptIdentityDigest(prepareID, graphID, projectID, graphDigest string, createdAt time.Time) (string, error) {
	if createdAt.IsZero() || createdAt.Year() < 2000 || createdAt.Year() > 9999 {
		return "", errors.New("prepared receipt creation time is out of bounds")
	}
	payload, err := json.Marshal([]string{"ao.prepared-receipt/1", prepareID, graphID, projectID, graphDigest, createdAt.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func runtimeState(state domain.TaskState) bool {
	switch state {
	case domain.TaskAssigned, domain.TaskRunning, domain.TaskReview, domain.TaskCompleted, domain.TaskFailed:
		return true
	default:
		return false
	}
}

func (s *Store) GetExecutionGraph(ctx context.Context, id string) (domain.ExecutionGraph, bool, error) {
	return readExecutionGraph(ctx, s.readDB, id)
}

func (s *Store) ListActiveGraphs(ctx context.Context) ([]domain.ExecutionGraph, error) {
	rows, err := s.readDB.QueryContext(ctx, `SELECT graph_json FROM execution_graphs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var graphs []domain.ExecutionGraph
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var graph domain.ExecutionGraph
		if err := json.Unmarshal([]byte(body), &graph); err != nil {
			return nil, err
		}
		active := false
		for _, task := range graph.Tasks {
			if task.State != domain.TaskCompleted && task.State != domain.TaskCancelled {
				active = true
				break
			}
		}
		if active {
			graphs = append(graphs, graph)
		}
	}
	return graphs, rows.Err()
}

func readExecutionGraph(ctx context.Context, db gen.DBTX, id string) (domain.ExecutionGraph, bool, error) {
	var body string
	err := db.QueryRowContext(ctx, `SELECT graph_json FROM execution_graphs WHERE id=?`, id).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExecutionGraph{}, false, nil
	}
	if err != nil {
		return domain.ExecutionGraph{}, false, err
	}
	var graph domain.ExecutionGraph
	if err := json.Unmarshal([]byte(body), &graph); err != nil {
		return domain.ExecutionGraph{}, false, err
	}
	return graph, true, nil
}

func writeExecutionGraph(ctx context.Context, db gen.DBTX, old, next domain.ExecutionGraph, at time.Time) error {
	if err := domain.ValidateExecutionGraphTransition(old, next); err != nil {
		return err
	}
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `UPDATE execution_graphs SET revision=?,graph_json=?,updated_at=? WHERE id=? AND revision=?`, next.Revision, string(body), at.UTC(), next.ID, old.Revision)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return execution.ErrGraphConflict
	}
	return nil
}

// mutateExecutionGraph serializes one transition and commits its graph revision
// with its auxiliary attempt/resource rows. False means state or attempt changed.
func (s *Store) mutateExecutionGraph(ctx context.Context, graphID string, at time.Time, mutate func(*domain.ExecutionGraph, gen.DBTX) (bool, error)) (bool, error) {
	if err := s.writeMu.LockContext(ctx); err != nil {
		return false, err
	}
	defer s.writeMu.Unlock()
	changed := false
	err := s.inTxDB(ctx, "update execution graph", func(_ *gen.Queries, db gen.DBTX) error {
		old, found, err := readExecutionGraph(ctx, db, graphID)
		if err != nil || !found {
			return err
		}
		// Assignment pointers and attempt slices must not alias old: transition
		// validation compares the persisted snapshot with the proposed revision.
		encoded, err := json.Marshal(old)
		if err != nil {
			return err
		}
		var next domain.ExecutionGraph
		if err := json.Unmarshal(encoded, &next); err != nil {
			return err
		}
		changed, err = mutate(&next, db)
		if err != nil || !changed {
			return err
		}
		next.Revision++
		return writeExecutionGraph(ctx, db, old, next, at)
	})
	return changed && err == nil, err
}

func taskIndex(graph *domain.ExecutionGraph, id string) int {
	for i := range graph.Tasks {
		if graph.Tasks[i].ID == id {
			return i
		}
	}
	return -1
}

func hardDepsComplete(graph *domain.ExecutionGraph, task domain.ExecutionTask) bool {
	for _, dep := range task.Dependencies {
		i := taskIndex(graph, dep)
		if i < 0 || graph.Tasks[i].State != domain.TaskCompleted {
			return false
		}
	}
	return true
}

func (s *Store) MarkReady(ctx context.Context, graphID, taskID string, at time.Time) (bool, error) {
	return s.mutateExecutionGraph(ctx, graphID, at, func(graph *domain.ExecutionGraph, _ gen.DBTX) (bool, error) {
		i := taskIndex(graph, taskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if !hardDepsComplete(graph, *task) || task.Assignment != nil {
			return false, nil
		}
		if task.State == domain.TaskFailed {
			if len(task.Attempts) == 0 || !task.Attempts[len(task.Attempts)-1].Retryable {
				return false, nil
			}
		} else if task.State != domain.TaskPending && task.State != domain.TaskBlocked {
			return false, nil
		}
		task.State = domain.TaskReady
		return true, nil
	})
}

// CancelTask retires work that has no active owner. Active and review tasks
// require a fenced worker outcome or review decision before cancellation.
func (s *Store) CancelTask(ctx context.Context, graphID, taskID string, at time.Time) (bool, error) {
	return s.mutateExecutionGraph(ctx, graphID, at, func(graph *domain.ExecutionGraph, _ gen.DBTX) (bool, error) {
		i := taskIndex(graph, taskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if task.Assignment != nil {
			return false, nil
		}
		switch task.State {
		case domain.TaskPending, domain.TaskReady, domain.TaskBlocked, domain.TaskFailed:
			task.State = domain.TaskCancelled
			return true, nil
		default:
			return false, nil
		}
	})
}

func sameResources(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]int, len(a))
	for _, resource := range a {
		set[resource]++
	}
	for _, resource := range b {
		set[resource]--
		if set[resource] < 0 {
			return false
		}
	}
	return true
}

// canonicalResource is the exclusive key within one project. Slash spelling
// and dot segments are equivalent on every host; Windows path case is folded.
func canonicalResource(resource string) string {
	key := path.Clean(strings.ReplaceAll(resource, `\`, "/"))
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return key
}

func (s *Store) ClaimTask(ctx context.Context, claim execution.Claim) (bool, error) {
	if claim.AttemptID == "" || claim.AgentID == "" || claim.Provider == "" || claim.ContractFingerprint == "" || !claim.LeaseExpiresAt.After(claim.At) {
		return false, fmt.Errorf("execution claim: attempt, agent, provider, contract fingerprint and valid lease required")
	}
	return s.mutateExecutionGraph(ctx, claim.GraphID, claim.At, func(graph *domain.ExecutionGraph, db gen.DBTX) (bool, error) {
		i := taskIndex(graph, claim.TaskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if task.State != domain.TaskReady || task.Assignment != nil || !hardDepsComplete(graph, *task) || !sameResources(task.Resources, claim.Resources) || execution.TaskContractFingerprint(*graph, *task) != claim.ContractFingerprint {
			return false, nil
		}
		var existing int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_attempts WHERE attempt_id=?`, claim.AttemptID).Scan(&existing); err != nil {
			return false, err
		}
		if existing != 0 {
			return false, nil
		}
		var active int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_attempts WHERE state IN ('assigned','running')`).Scan(&active); err != nil {
			return false, err
		}
		if claim.MaxConcurrent > 0 && active >= claim.MaxConcurrent {
			return false, nil
		}
		if claim.MaxPerProvider > 0 {
			var providerActive int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_attempts WHERE provider=? AND state IN ('assigned','running')`, claim.Provider).Scan(&providerActive); err != nil {
				return false, err
			}
			if providerActive >= claim.MaxPerProvider {
				return false, nil
			}
		}
		for _, resource := range claim.Resources {
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_resource_claims WHERE project_id=? AND resource_key=?`, string(graph.ProjectID), canonicalResource(resource)).Scan(&existing); err != nil {
				return false, err
			}
			if existing != 0 {
				return false, nil
			}
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO execution_attempts(attempt_id,graph_id,task_id,project_id,provider,state,started_at) VALUES(?,?,?,?,?,'assigned',?)`, claim.AttemptID, graph.ID, task.ID, string(graph.ProjectID), claim.Provider, claim.At.UTC()); err != nil {
			return false, err
		}
		for _, resource := range claim.Resources {
			if _, err := db.ExecContext(ctx, `INSERT INTO execution_resource_claims(project_id,resource_key,graph_id,task_id,attempt_id,claimed_at,lease_expires_at) VALUES(?,?,?,?,?,?,?)`, string(graph.ProjectID), canonicalResource(resource), graph.ID, task.ID, claim.AttemptID, claim.At.UTC(), claim.LeaseExpiresAt.UTC()); err != nil {
				return false, err
			}
		}
		task.State = domain.TaskAssigned
		task.Assignment = &domain.TaskAssignment{AgentID: claim.AgentID, Provider: claim.Provider, AttemptID: claim.AttemptID, AssignedAt: claim.At.UTC()}
		task.Attempts = append(task.Attempts, domain.TaskAttempt{ID: claim.AttemptID, AgentID: claim.AgentID, StartedAt: claim.At.UTC()})
		return true, nil
	})
}

func (s *Store) MarkRunning(ctx context.Context, graphID, taskID, attemptID string, sessionID domain.SessionID, at time.Time) (bool, error) {
	if sessionID == "" {
		return false, fmt.Errorf("execution session ID required")
	}
	return s.mutateExecutionGraph(ctx, graphID, at, func(graph *domain.ExecutionGraph, db gen.DBTX) (bool, error) {
		i := taskIndex(graph, taskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if task.State != domain.TaskAssigned || task.Assignment == nil || task.Assignment.AttemptID != attemptID || len(task.Attempts) == 0 || task.Attempts[len(task.Attempts)-1].ID != attemptID {
			return false, nil
		}
		task.State = domain.TaskRunning
		task.Assignment.SessionID = &sessionID
		task.Attempts[len(task.Attempts)-1].SessionID = &sessionID
		result, err := db.ExecContext(ctx, `UPDATE execution_attempts SET state='running' WHERE attempt_id=? AND graph_id=? AND task_id=? AND state='assigned'`, attemptID, graphID, taskID)
		if err != nil {
			return false, err
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return false, err
		}
		return true, nil
	})
}

func (s *Store) MarkReview(ctx context.Context, graphID, taskID, attemptID string, at time.Time) (bool, error) {
	return s.mutateExecutionGraph(ctx, graphID, at, func(graph *domain.ExecutionGraph, db gen.DBTX) (bool, error) {
		i := taskIndex(graph, taskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if task.State != domain.TaskRunning || task.Assignment == nil || task.Assignment.AttemptID != attemptID {
			return false, nil
		}
		task.State = domain.TaskReview
		result, err := db.ExecContext(ctx, `UPDATE execution_attempts SET state='review' WHERE attempt_id=? AND graph_id=? AND task_id=? AND state='running'`, attemptID, graphID, taskID)
		if err != nil {
			return false, err
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return false, err
		}
		return true, nil
	})
}

func (s *Store) ResolveReview(ctx context.Context, graphID, taskID, attemptID string, decision execution.ReviewDecision, at time.Time) (bool, error) {
	return s.ResolveReviewWithEvidence(ctx, graphID, taskID, attemptID, decision, domain.TaskReviewRecord{}, at)
}

// ResolveReviewWithEvidence atomically finishes the fenced attempt and appends
// the coordinator's review record to the durable graph snapshot.
func (s *Store) ResolveReviewWithEvidence(ctx context.Context, graphID, taskID, attemptID string, decision execution.ReviewDecision, evidence domain.TaskReviewRecord, at time.Time) (bool, error) {
	if decision != execution.ReviewAccept && decision != execution.ReviewRevise && decision != execution.ReviewReject {
		return false, execution.ErrInvalidDecision
	}
	if err := domain.ValidateTaskReviewEvidence(evidence); err != nil {
		return false, err
	}
	return s.mutateExecutionGraph(ctx, graphID, at, func(graph *domain.ExecutionGraph, db gen.DBTX) (bool, error) {
		i := taskIndex(graph, taskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if task.State != domain.TaskReview || task.Assignment == nil || task.Assignment.AttemptID != attemptID || len(task.Attempts) == 0 || task.Attempts[len(task.Attempts)-1].ID != attemptID {
			return false, nil
		}
		if decision == execution.ReviewRevise && len(task.Attempts) >= execution.DefaultMaxAttempts {
			return false, execution.ErrReviewAttemptLimit
		}
		switch decision {
		case execution.ReviewAccept:
			task.State = domain.TaskCompleted
		case execution.ReviewRevise:
			task.State = domain.TaskReady
		case execution.ReviewReject:
			task.State = domain.TaskFailed
		}
		changed, err := finishAttempt(ctx, db, task, attemptID, at, decision == execution.ReviewRevise, "")
		if err != nil || !changed {
			return changed, err
		}
		task.Reviews = append(task.Reviews, domain.TaskReviewRecord{
			AttemptID: attemptID, Decision: string(decision), ReviewedAt: at.UTC(),
			SourceHead: evidence.SourceHead, PRURL: evidence.PRURL,
			ValidationSummary: evidence.ValidationSummary, EvidenceRefs: evidence.EvidenceRefs,
		})
		return true, nil
	})
}

func (s *Store) FailTask(ctx context.Context, graphID, taskID, attemptID, reason string, at time.Time) (bool, error) {
	return s.mutateExecutionGraph(ctx, graphID, at, func(graph *domain.ExecutionGraph, db gen.DBTX) (bool, error) {
		i := taskIndex(graph, taskID)
		if i < 0 {
			return false, nil
		}
		task := &graph.Tasks[i]
		if (task.State != domain.TaskAssigned && task.State != domain.TaskRunning && task.State != domain.TaskReview) || task.Assignment == nil || task.Assignment.AttemptID != attemptID || len(task.Attempts) == 0 || task.Attempts[len(task.Attempts)-1].ID != attemptID {
			return false, nil
		}
		task.State = domain.TaskFailed
		return finishAttempt(ctx, db, task, attemptID, at, true, reason)
	})
}

func finishAttempt(ctx context.Context, db gen.DBTX, task *domain.ExecutionTask, attemptID string, at time.Time, retryable bool, reason string) (bool, error) {
	result, err := db.ExecContext(ctx, `UPDATE execution_attempts SET state='finished',finished_at=? WHERE attempt_id=? AND state<>'finished'`, at.UTC(), attemptID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if count != 1 {
		return false, nil
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM execution_resource_claims WHERE attempt_id=?`, attemptID); err != nil {
		return false, err
	}
	last := &task.Attempts[len(task.Attempts)-1]
	finished := at.UTC()
	last.FinishedAt = &finished
	last.Retryable = retryable
	last.Error = reason
	if task.State == domain.TaskReady && !retryable {
		return false, fmt.Errorf("execution revision requires retryable attempt")
	}
	task.Assignment = nil
	return true, nil
}
