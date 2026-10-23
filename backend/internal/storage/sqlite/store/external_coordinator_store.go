package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func (s *Store) GetExternalCoordinator(ctx context.Context, projectID string) (domain.ExternalCoordinatorLease, bool, error) {
	var owner, name string
	var seen, expires int64
	err := s.readDB.QueryRowContext(ctx, `SELECT owner_id, display_name, last_seen_at_ms, expires_at_ms
		FROM external_coordinator_leases WHERE project_id = ?`, projectID).Scan(&owner, &name, &seen, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExternalCoordinatorLease{}, false, nil
	}
	if err != nil {
		return domain.ExternalCoordinatorLease{}, false, fmt.Errorf("get external coordinator: %w", err)
	}
	return domain.ExternalCoordinatorLease{
		ProjectID: domain.ProjectID(projectID), OwnerID: owner, DisplayName: name,
		LastSeenAt: time.UnixMilli(seen).UTC(), ExpiresAt: time.UnixMilli(expires).UTC(),
	}, true, nil
}

// ClaimExternalCoordinator is a serialized claim or same-owner renewal.
// A different unexpired owner and any nonterminated AO orchestrator reject it.
func (s *Store) ClaimExternalCoordinator(
	ctx context.Context, projectID, ownerID, displayName string, now, expires time.Time,
) (domain.ExternalCoordinatorLease, error) {
	if err := s.writeMu.LockContext(ctx); err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var projectExists int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE id = ? AND archived_at IS NULL)`, projectID).Scan(&projectExists); err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	if projectExists == 0 {
		return domain.ExternalCoordinatorLease{}, domain.ErrExternalProjectNotFound
	}
	var aoActive int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions
		WHERE project_id = ? AND kind = 'orchestrator' AND is_terminated = 0)`, projectID).Scan(&aoActive); err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	if aoActive != 0 {
		return domain.ExternalCoordinatorLease{}, domain.ErrAOOrchestratorActive
	}
	var currentOwner string
	var currentExpiry int64
	err = tx.QueryRowContext(ctx, `SELECT owner_id, expires_at_ms FROM external_coordinator_leases
		WHERE project_id = ?`, projectID).Scan(&currentOwner, &currentExpiry)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return domain.ExternalCoordinatorLease{}, err
	}
	if err == nil && currentOwner != ownerID && currentExpiry > now.UnixMilli() {
		return domain.ExternalCoordinatorLease{}, domain.ErrExternalCoordinatorOwned
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO external_coordinator_leases
		(project_id, owner_id, display_name, last_seen_at_ms, expires_at_ms)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (project_id) DO UPDATE SET
			owner_id = excluded.owner_id,
			display_name = excluded.display_name,
			last_seen_at_ms = excluded.last_seen_at_ms,
			expires_at_ms = excluded.expires_at_ms`,
		projectID, ownerID, displayName, now.UnixMilli(), expires.UnixMilli())
	if err != nil {
		if strings.Contains(err.Error(), "AO_ORCHESTRATOR_ACTIVE") {
			return domain.ExternalCoordinatorLease{}, domain.ErrAOOrchestratorActive
		}
		return domain.ExternalCoordinatorLease{}, fmt.Errorf("claim external coordinator: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	return domain.ExternalCoordinatorLease{
		ProjectID: domain.ProjectID(projectID), OwnerID: ownerID, DisplayName: displayName,
		LastSeenAt: now.UTC(), ExpiresAt: expires.UTC(),
	}, nil
}

// ClearExternalCoordinator is idempotent for an absent lease but cannot clear
// another owner's registration.
func (s *Store) ClearExternalCoordinator(ctx context.Context, projectID, ownerID string) error {
	if err := s.writeMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentOwner string
	err = tx.QueryRowContext(ctx, `SELECT owner_id FROM external_coordinator_leases
		WHERE project_id = ?`, projectID).Scan(&currentOwner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if currentOwner != ownerID {
		return domain.ErrExternalCoordinatorOwned
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM external_coordinator_leases
		WHERE project_id = ? AND owner_id = ?`, projectID, ownerID); err != nil {
		return err
	}
	return tx.Commit()
}
