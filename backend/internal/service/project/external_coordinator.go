package project

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

const ExternalCoordinatorLeaseDuration = 5 * time.Minute

type ExternalCoordinatorClaim struct {
	OwnerID     string `json:"ownerId"`
	DisplayName string `json:"displayName"`
}

type externalCoordinatorStore interface {
	GetExternalCoordinator(context.Context, string) (domain.ExternalCoordinatorLease, bool, error)
	ClaimExternalCoordinator(context.Context, string, string, string, time.Time, time.Time) (domain.ExternalCoordinatorLease, error)
	ClearExternalCoordinator(context.Context, string, string) error
}

func (m *Service) externalCoordinatorStore() (externalCoordinatorStore, error) {
	store, ok := m.store.(externalCoordinatorStore)
	if !ok {
		return nil, apierr.Internal("EXTERNAL_COORDINATOR_UNAVAILABLE", "External coordinator storage is unavailable")
	}
	return store, nil
}

func (m *Service) requireActiveProject(ctx context.Context, id domain.ProjectID) error {
	if err := validateProjectID(id); err != nil {
		return err
	}
	row, found, err := m.store.GetProject(ctx, string(id))
	if err != nil {
		return apierr.Internal("PROJECT_LOAD_FAILED", "Failed to load project")
	}
	if !found || !row.ArchivedAt.IsZero() {
		return apierr.NotFound("PROJECT_NOT_FOUND", "Project not found")
	}
	return nil
}

// GetExternalCoordinator returns the current live external registration. An
// expired row is retained only as historical storage and has no authority.
func (m *Service) GetExternalCoordinator(ctx context.Context, id domain.ProjectID) (*domain.ExternalCoordinatorLease, error) {
	if err := m.requireActiveProject(ctx, id); err != nil {
		return nil, err
	}
	store, err := m.externalCoordinatorStore()
	if err != nil {
		return nil, err
	}
	lease, found, err := store.GetExternalCoordinator(ctx, string(id))
	if err != nil {
		return nil, apierr.Internal("EXTERNAL_COORDINATOR_READ_FAILED", "Failed to load external coordinator")
	}
	if !found || !lease.Active(m.clock()) {
		return nil, nil
	}
	return &lease, nil
}

// ClaimExternalCoordinator atomically registers or renews a single owner.
// It never creates a session or changes worker report routing.
func (m *Service) ClaimExternalCoordinator(ctx context.Context, id domain.ProjectID, in ExternalCoordinatorClaim) (domain.ExternalCoordinatorLease, error) {
	owner := strings.TrimSpace(in.OwnerID)
	name := strings.TrimSpace(in.DisplayName)
	if utf8.RuneCountInString(owner) < 1 || utf8.RuneCountInString(owner) > 120 ||
		utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > 120 {
		return domain.ExternalCoordinatorLease{}, apierr.Invalid("INVALID_EXTERNAL_COORDINATOR", "ownerId and displayName must be 1 to 120 characters", nil)
	}
	if err := m.requireActiveProject(ctx, id); err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	store, err := m.externalCoordinatorStore()
	if err != nil {
		return domain.ExternalCoordinatorLease{}, err
	}
	now := m.clock().UTC()
	lease, err := store.ClaimExternalCoordinator(ctx, string(id), owner, name, now, now.Add(ExternalCoordinatorLeaseDuration))
	switch {
	case errors.Is(err, domain.ErrExternalCoordinatorOwned):
		return domain.ExternalCoordinatorLease{}, apierr.Conflict("EXTERNAL_COORDINATOR_OWNED", "Another external coordinator owns this project", nil)
	case errors.Is(err, domain.ErrAOOrchestratorActive):
		return domain.ExternalCoordinatorLease{}, apierr.Conflict("AO_ORCHESTRATOR_ACTIVE", "An AO orchestrator session already owns this project", nil)
	case errors.Is(err, domain.ErrExternalProjectNotFound):
		return domain.ExternalCoordinatorLease{}, apierr.NotFound("PROJECT_NOT_FOUND", "Project not found")
	case err != nil:
		return domain.ExternalCoordinatorLease{}, apierr.Internal("EXTERNAL_COORDINATOR_CLAIM_FAILED", "Failed to claim external coordinator")
	}
	return lease, nil
}

func (m *Service) ClearExternalCoordinator(ctx context.Context, id domain.ProjectID, ownerID string) error {
	owner := strings.TrimSpace(ownerID)
	if owner == "" || utf8.RuneCountInString(owner) > 120 {
		return apierr.Invalid("INVALID_EXTERNAL_COORDINATOR", "ownerId must be 1 to 120 characters", nil)
	}
	if err := m.requireActiveProject(ctx, id); err != nil {
		return err
	}
	store, err := m.externalCoordinatorStore()
	if err != nil {
		return err
	}
	err = store.ClearExternalCoordinator(ctx, string(id), owner)
	if errors.Is(err, domain.ErrExternalCoordinatorOwned) {
		return apierr.Conflict("EXTERNAL_COORDINATOR_OWNED", "Another external coordinator owns this project", nil)
	}
	if err != nil {
		return apierr.Internal("EXTERNAL_COORDINATOR_CLEAR_FAILED", "Failed to clear external coordinator")
	}
	return nil
}
