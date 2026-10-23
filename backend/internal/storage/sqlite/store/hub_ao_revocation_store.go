package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/service/execution"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// hubAORevocationInput is private and does not assert that its source was
// authenticated. A future protected handler must authenticate and validate a
// Hub event before constructing it; no production caller exists in this slice.
type hubAORevocationInput struct {
	EventID        string
	EventDigest    string
	GraphID        string
	ActivationID   string
	HostID         string
	StoreID        string
	HostGeneration int64
	AuthorityEpoch int64
}

type hubAORevocationRecord struct {
	hubAORevocationInput
	FencedAt time.Time
}

func (input hubAORevocationInput) validate() error {
	for _, id := range []string{input.EventID, input.GraphID, input.ActivationID, input.HostID, input.StoreID} {
		if id == "" || len(id) > 128 || strings.TrimSpace(id) != id || strings.ContainsAny(id, "\r\n\x00") {
			return errors.New("Hub AO revocation: invalid identity")
		}
	}
	if !validHubAOActivationDigest(input.EventDigest) || input.HostGeneration <= 0 ||
		input.HostGeneration > 9007199254740991 || input.AuthorityEpoch < 0 || input.AuthorityEpoch > 9007199254740991 {
		return errors.New("Hub AO revocation: invalid digest, generation, or epoch")
	}
	return nil
}

func readHubAORevocation(ctx context.Context, db gen.DBTX, input hubAORevocationInput) (hubAORevocationRecord, bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT event_id,event_digest,graph_id,activation_id,host_id,store_id,host_generation,authority_epoch,fenced_at
		FROM hub_ao_revocation_events WHERE event_id=? OR graph_id=? OR activation_id=?`,
		input.EventID, input.GraphID, input.ActivationID)
	if err != nil {
		return hubAORevocationRecord{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return hubAORevocationRecord{}, false, rows.Err()
	}
	var record hubAORevocationRecord
	if err := rows.Scan(&record.EventID, &record.EventDigest, &record.GraphID, &record.ActivationID,
		&record.HostID, &record.StoreID, &record.HostGeneration, &record.AuthorityEpoch, &record.FencedAt); err != nil {
		return hubAORevocationRecord{}, false, err
	}
	if rows.Next() {
		return hubAORevocationRecord{}, false, execution.ErrGraphConflict
	}
	return record, true, rows.Err()
}

// commitHubAORevocation is an inert SQLite primitive with no production
// caller. It checks exact persisted identity but does not authenticate a Hub
// event. A future protected handler must do that before invoking this method.
// This transaction never mutates ASSIGNED attempts or their resource claims.
func (s *Store) commitHubAORevocation(ctx context.Context, input hubAORevocationInput) (hubAORevocationRecord, bool, error) {
	var record hubAORevocationRecord
	if err := input.validate(); err != nil {
		return record, false, err
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return record, false, err
	}
	defer s.writeMu.Unlock()
	created := false
	err := s.inTxDB(ctx, "commit Hub AO revocation", func(_ *gen.Queries, db gen.DBTX) error {
		prior, found, err := readHubAORevocation(ctx, db, input)
		if err != nil {
			return err
		}
		if found {
			if prior.hubAORevocationInput != input {
				return execution.ErrGraphConflict
			}
			var fence sql.NullTime
			if err := db.QueryRowContext(ctx, `SELECT fenced_at FROM hub_ao_activations WHERE graph_id=? AND activation_id=?`, input.GraphID, input.ActivationID).Scan(&fence); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return execution.ErrGraphConflict
				}
				return err
			}
			if !fence.Valid || !fence.Time.Equal(prior.FencedAt) {
				return execution.ErrGraphConflict
			}
			record = prior // Lost response returns the original event receipt.
			return nil
		}
		var storeID, hostID string
		var generation, epoch int64
		if err := db.QueryRowContext(ctx, `SELECT store_id,host_id,host_generation,authority_epoch FROM hub_ao_control_identity WHERE singleton=1`).
			Scan(&storeID, &hostID, &generation, &epoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return execution.ErrGraphConflict
			}
			return err
		}
		if storeID != input.StoreID || hostID != input.HostID || generation != input.HostGeneration || epoch != input.AuthorityEpoch {
			return execution.ErrGraphConflict
		}
		var activationID, ledgerHost, ledgerStore, state string
		var ledgerGeneration, ledgerEpoch int64
		var fence sql.NullTime
		if err := db.QueryRowContext(ctx, `SELECT a.activation_id,a.host_id,a.store_id,a.host_generation,a.authority_epoch,a.state,a.fenced_at
			FROM hub_ao_activations AS a JOIN hub_ao_graph_origins AS o ON o.graph_id=a.graph_id WHERE a.graph_id=?`, input.GraphID).
			Scan(&activationID, &ledgerHost, &ledgerStore, &ledgerGeneration, &ledgerEpoch, &state, &fence); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return execution.ErrGraphConflict
			}
			return err
		}
		if activationID != input.ActivationID || ledgerHost != input.HostID || ledgerStore != input.StoreID ||
			ledgerGeneration != input.HostGeneration || ledgerEpoch != input.AuthorityEpoch || state != "active" || fence.Valid {
			return execution.ErrGraphConflict
		}
		now := time.Now().UTC()
		updated, err := db.ExecContext(ctx, `UPDATE hub_ao_activations SET fenced_at=? WHERE graph_id=? AND activation_id=? AND fenced_at IS NULL`,
			now, input.GraphID, input.ActivationID)
		if err != nil {
			return err
		}
		if count, err := updated.RowsAffected(); err != nil || count != 1 {
			return fmt.Errorf("Hub AO revocation: activation changed: %w", execution.ErrGraphConflict)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO hub_ao_revocation_events(
			event_id,event_digest,graph_id,activation_id,host_id,store_id,host_generation,authority_epoch,fenced_at)
			VALUES(?,?,?,?,?,?,?,?,?)`, input.EventID, input.EventDigest, input.GraphID, input.ActivationID,
			input.HostID, input.StoreID, input.HostGeneration, input.AuthorityEpoch, now); err != nil {
			return err
		}
		record = hubAORevocationRecord{hubAORevocationInput: input, FencedAt: now}
		created = true
		return nil
	})
	return record, created && err == nil, err
}
