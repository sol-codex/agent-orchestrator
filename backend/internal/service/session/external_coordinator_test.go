package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

type externalLeaseStore struct {
	*fakeStore
	lease domain.ExternalCoordinatorLease
}

func (s *externalLeaseStore) GetExternalCoordinator(context.Context, string) (domain.ExternalCoordinatorLease, bool, error) {
	return s.lease, true, nil
}

func TestAOOrchestratorPreflightRejectsLiveExternalCoordinator(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	store := &externalLeaseStore{fakeStore: newFakeStore(), lease: domain.ExternalCoordinatorLease{
		ProjectID: "p1", OwnerID: "codex-desktop", ExpiresAt: now.Add(time.Minute),
	}}
	svc := &Service{store: store, clock: func() time.Time { return now }}
	err := svc.rejectExternalCoordinator(context.Background(), "p1")
	var apiError *apierr.Error
	if !errors.As(err, &apiError) || apiError.Kind != apierr.KindConflict || apiError.Code != "EXTERNAL_COORDINATOR_ACTIVE" {
		t.Fatalf("live lease must block AO orchestrator: %v", err)
	}
	svc.clock = func() time.Time { return now.Add(2 * time.Minute) }
	if err := svc.rejectExternalCoordinator(context.Background(), "p1"); err != nil {
		t.Fatalf("expired lease must not block AO orchestrator: %v", err)
	}
}
