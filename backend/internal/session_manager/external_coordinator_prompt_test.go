package sessionmanager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type promptLeaseStore struct {
	*fakeStore
	lease domain.ExternalCoordinatorLease
}

func (s *promptLeaseStore) GetExternalCoordinator(context.Context, string) (domain.ExternalCoordinatorLease, bool, error) {
	return s.lease, true, nil
}

func TestWorkerPromptReadsOnlyLiveExternalCoordinatorLease(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	store := &promptLeaseStore{fakeStore: newFakeStore(), lease: domain.ExternalCoordinatorLease{
		ProjectID: "mer", OwnerID: "desktop", ExpiresAt: now.Add(time.Minute),
	}}
	store.projects["mer"] = domain.ProjectRecord{ID: "mer", Path: t.TempDir()}
	manager := &Manager{store: store, clock: func() time.Time { return now }}
	prompt, err := manager.buildSystemPrompt(context.Background(), domain.KindWorker, "mer", "mer-worker", "", false)
	if err != nil || !strings.Contains(prompt, "## External Coordinator") {
		t.Fatalf("live external coordinator prompt: err=%v prompt=%q", err, prompt)
	}
	manager.clock = func() time.Time { return now.Add(2 * time.Minute) }
	prompt, err = manager.buildSystemPrompt(context.Background(), domain.KindWorker, "mer", "mer-worker", "", false)
	if err != nil || strings.Contains(prompt, "## External Coordinator") {
		t.Fatalf("expired external coordinator prompt: err=%v prompt=%q", err, prompt)
	}
}
