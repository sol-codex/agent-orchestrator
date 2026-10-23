package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// hubAOActivationInput is deliberately private. A future protected control
// caller must first verify the #12 permit and #13 online currentness proof and
// obtain host/store/generation/epoch from server-owned state, never request JSON.
// No production caller constructs this input in this source slice.
type hubAOActivationInput struct {
	GraphID                string
	ProjectID              string
	PrepareID              string
	GraphBytesSHA256       string
	ReceiptIdentitySHA256  string
	GraphContractDigest    string
	HubAOPlanBindingDigest string
	GrantID                string
	GrantDigest            string
	RedemptionID           string
	PermitDigest           string
	PermitUseBy            string
	ActivationAttemptID    string
	CheckID                string
	CurrentnessDigest      string
	CurrentnessCheckedAt   string
	CurrentnessUseBy       string
	HostID                 string
	StoreID                string
	HostGeneration         int64
	AuthorityEpoch         int64
}

type hubAOActivationRecord struct {
	hubAOActivationInput
	PreparedCreatedAt time.Time
	ActivationID      string
	GraphRevision     int64
	State             string
	ActivatedAt       time.Time
}

func validHubAOActivationDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func hubAOActivationInstant(value string) (time.Time, error) {
	const layout = "2006-01-02T15:04:05.000Z"
	parsed, err := time.Parse(layout, value)
	if err != nil || parsed.Format(layout) != value {
		return time.Time{}, errors.New("Hub AO activation: invalid UTC millisecond instant")
	}
	return parsed, nil
}

func (input hubAOActivationInput) validate(now time.Time) error {
	for _, id := range []string{input.GraphID, input.ProjectID, input.PrepareID, input.GrantID,
		input.RedemptionID, input.ActivationAttemptID, input.CheckID, input.HostID, input.StoreID} {
		if id == "" || len(id) > 128 || strings.TrimSpace(id) != id {
			return errors.New("Hub AO activation: invalid identity")
		}
	}
	if err := validatePrepareID(input.PrepareID); err != nil {
		return err
	}
	for _, digest := range []string{input.GraphBytesSHA256, input.ReceiptIdentitySHA256,
		input.GraphContractDigest, input.HubAOPlanBindingDigest,
		input.GrantDigest, input.PermitDigest, input.CurrentnessDigest} {
		if !validHubAOActivationDigest(digest) {
			return errors.New("Hub AO activation: invalid digest")
		}
	}
	if input.HostGeneration <= 0 || input.HostGeneration > 9007199254740991 ||
		input.AuthorityEpoch < 0 || input.AuthorityEpoch > 9007199254740991 {
		return errors.New("Hub AO activation: invalid generation or epoch")
	}
	checked, err := hubAOActivationInstant(input.CurrentnessCheckedAt)
	if err != nil {
		return err
	}
	currentnessUseBy, err := hubAOActivationInstant(input.CurrentnessUseBy)
	if err != nil {
		return err
	}
	permitUseBy, err := hubAOActivationInstant(input.PermitUseBy)
	if err != nil || !currentnessUseBy.After(checked) || currentnessUseBy.Sub(checked) > 5*time.Second ||
		currentnessUseBy.After(permitUseBy) || now.Before(checked) || !now.Before(currentnessUseBy) || !now.Before(permitUseBy) {
		return errors.New("Hub AO activation: permit or online currentness stale")
	}
	return nil
}

func readHubAOActivation(ctx context.Context, db gen.DBTX, input hubAOActivationInput) (hubAOActivationRecord, bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT graph_id,project_id,prepare_id,prepared_created_at,graph_bytes_sha256,receipt_identity_sha256,graph_contract_digest,hub_ao_plan_binding_digest,
		grant_id,grant_digest,redemption_id,permit_digest,permit_use_by,activation_attempt_id,check_id,currentness_digest,
		currentness_checked_at,currentness_use_by,host_id,store_id,host_generation,authority_epoch,activation_id,graph_revision,state,activated_at
		FROM hub_ao_activations WHERE graph_id=? OR prepare_id=? OR grant_id=? OR redemption_id=? OR activation_attempt_id=? OR check_id=?`,
		input.GraphID, input.PrepareID, input.GrantID, input.RedemptionID, input.ActivationAttemptID, input.CheckID)
	if err != nil {
		return hubAOActivationRecord{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return hubAOActivationRecord{}, false, rows.Err()
	}
	var record hubAOActivationRecord
	if err := rows.Scan(&record.GraphID, &record.ProjectID, &record.PrepareID, &record.PreparedCreatedAt,
		&record.GraphBytesSHA256, &record.ReceiptIdentitySHA256, &record.GraphContractDigest, &record.HubAOPlanBindingDigest,
		&record.GrantID, &record.GrantDigest,
		&record.RedemptionID, &record.PermitDigest, &record.PermitUseBy, &record.ActivationAttemptID,
		&record.CheckID, &record.CurrentnessDigest, &record.CurrentnessCheckedAt, &record.CurrentnessUseBy,
		&record.HostID, &record.StoreID, &record.HostGeneration, &record.AuthorityEpoch,
		&record.ActivationID, &record.GraphRevision, &record.State, &record.ActivatedAt); err != nil {
		return hubAOActivationRecord{}, false, err
	}
	if rows.Next() {
		return hubAOActivationRecord{}, false, execution.ErrGraphConflict
	}
	return record, true, rows.Err()
}

// commitHubAOActivation is an inert store primitive with no service/HTTP/pipe
// caller. It does not authenticate the input. Once integrated, proof checks and
// a fresh online Hub response must precede this transaction. Its commit is the
// sole prepared-to-active linearization point; it never wakes a scheduler.
func (s *Store) commitHubAOActivation(ctx context.Context, input hubAOActivationInput) (hubAOActivationRecord, bool, error) {
	var record hubAOActivationRecord
	if err := s.writeMu.LockContext(ctx); err != nil {
		return record, false, err
	}
	defer s.writeMu.Unlock()
	created := false
	err := s.inTxDB(ctx, "commit Hub AO activation", func(_ *gen.Queries, db gen.DBTX) error {
		prior, found, err := readHubAOActivation(ctx, db, input)
		if err != nil {
			return err
		}
		if found {
			if prior.hubAOActivationInput != input || prior.State != "active" || prior.GraphRevision != 1 {
				return execution.ErrGraphConflict
			}
			record = prior // Lost response: return the same receipt, even after expiry.
			return nil
		}
		now := time.Now().UTC()
		if err := input.validate(now); err != nil {
			return err
		}
		var storeID, hostID string
		var generation, epoch int64
		if err := db.QueryRowContext(ctx, `SELECT store_id,host_id,host_generation,authority_epoch FROM hub_ao_control_identity WHERE singleton=1`).
			Scan(&storeID, &hostID, &generation, &epoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("Hub AO activation: attended enrollment absent")
			}
			return err
		}
		if storeID != input.StoreID || hostID != input.HostID || generation != input.HostGeneration || epoch != input.AuthorityEpoch {
			return execution.ErrGraphConflict
		}
		var preparedGraphID, preparedProjectID, preparedDigest, receiptDigest, graphJSON string
		var preparedCreatedAt time.Time
		if err := db.QueryRowContext(ctx, `SELECT id,project_id,graph_bytes_sha256,receipt_identity_sha256,graph_json,created_at
			FROM prepared_execution_graphs WHERE prepare_id=?`, input.PrepareID).
			Scan(&preparedGraphID, &preparedProjectID, &preparedDigest, &receiptDigest, &graphJSON, &preparedCreatedAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return execution.ErrGraphConflict
			}
			return err
		}
		verified, err := verifiedPreparedReceipt(input.PrepareID, preparedGraphID, preparedProjectID,
			preparedDigest, receiptDigest, graphJSON, preparedCreatedAt)
		if err != nil {
			return err
		}
		if verified.GraphID != input.GraphID || string(verified.ProjectID) != input.ProjectID ||
			verified.GraphBytesSHA256 != input.GraphBytesSHA256 || verified.ReceiptIdentitySHA256 != input.ReceiptIdentitySHA256 {
			return execution.ErrGraphConflict
		}
		var graph domain.ExecutionGraph
		if err := json.Unmarshal([]byte(graphJSON), &graph); err != nil {
			return err
		}
		if graph.Revision != 1 {
			return execution.ErrGraphConflict
		}
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return err
		}
		activationID := "ao-" + hex.EncodeToString(random[:])
		if _, err := db.ExecContext(ctx, `INSERT INTO execution_graphs(id,project_id,revision,graph_json,created_at,updated_at) VALUES(?,?,?,?,?,?)`,
			graph.ID, input.ProjectID, 1, graphJSON, now, now); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO hub_ao_activations(
			graph_id,project_id,prepare_id,prepared_created_at,graph_bytes_sha256,receipt_identity_sha256,graph_contract_digest,hub_ao_plan_binding_digest,
			grant_id,grant_digest,redemption_id,permit_digest,permit_use_by,activation_attempt_id,check_id,
			currentness_digest,currentness_checked_at,currentness_use_by,host_id,store_id,host_generation,authority_epoch,
			activation_id,graph_revision,state,activated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			input.GraphID, input.ProjectID, input.PrepareID, preparedCreatedAt,
			input.GraphBytesSHA256, input.ReceiptIdentitySHA256, input.GraphContractDigest, input.HubAOPlanBindingDigest,
			input.GrantID, input.GrantDigest,
			input.RedemptionID, input.PermitDigest, input.PermitUseBy, input.ActivationAttemptID, input.CheckID,
			input.CurrentnessDigest, input.CurrentnessCheckedAt, input.CurrentnessUseBy,
			input.HostID, input.StoreID, input.HostGeneration, input.AuthorityEpoch,
			activationID, 1, "active", now); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO hub_ao_graph_origins(graph_id) VALUES(?)`, input.GraphID); err != nil {
			return err
		}
		deleted, err := db.ExecContext(ctx, `DELETE FROM prepared_execution_graphs WHERE id=? AND prepare_id=?`, input.GraphID, input.PrepareID)
		if err != nil {
			return err
		}
		if count, err := deleted.RowsAffected(); err != nil || count != 1 {
			return fmt.Errorf("Hub AO activation: prepared row changed: %w", execution.ErrGraphConflict)
		}
		record = hubAOActivationRecord{hubAOActivationInput: input, PreparedCreatedAt: preparedCreatedAt,
			ActivationID: activationID, GraphRevision: 1, State: "active", ActivatedAt: now}
		created = true
		return nil
	})
	return record, created && err == nil, err
}
