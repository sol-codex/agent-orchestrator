package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestExternalCoordinatorClaimRenewExpiryAndClear(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "coord")
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	first, err := s.ClaimExternalCoordinator(ctx, "coord", "desktop-1", "Codex Desktop", now, now.Add(5*time.Minute))
	if err != nil || first.OwnerID != "desktop-1" {
		t.Fatalf("claim: lease=%+v err=%v", first, err)
	}
	if _, err := s.ClaimExternalCoordinator(ctx, "coord", "desktop-2", "Other", now.Add(time.Minute), now.Add(6*time.Minute)); !errors.Is(err, domain.ErrExternalCoordinatorOwned) {
		t.Fatalf("competing owner before expiry: %v", err)
	}
	renewed, err := s.ClaimExternalCoordinator(ctx, "coord", "desktop-1", "Codex Desktop", now.Add(time.Minute), now.Add(6*time.Minute))
	if err != nil || !renewed.ExpiresAt.After(first.ExpiresAt) {
		t.Fatalf("same-owner renewal: lease=%+v err=%v", renewed, err)
	}
	if err := s.ClearExternalCoordinator(ctx, "coord", "desktop-2"); !errors.Is(err, domain.ErrExternalCoordinatorOwned) {
		t.Fatalf("non-owner clear: %v", err)
	}
	if _, err := s.ClaimExternalCoordinator(ctx, "coord", "desktop-2", "Other", now.Add(7*time.Minute), now.Add(12*time.Minute)); err != nil {
		t.Fatalf("expired lease takeover: %v", err)
	}
	if err := s.ClearExternalCoordinator(ctx, "coord", "desktop-2"); err != nil {
		t.Fatalf("owner clear: %v", err)
	}
	if _, found, err := s.GetExternalCoordinator(ctx, "coord"); err != nil || found {
		t.Fatalf("cleared lease: found=%v err=%v", found, err)
	}
}

func TestExternalCoordinatorAndAOOrchestratorAreMutuallyExclusive(t *testing.T) {
	s := newTestStore(t)
	seedProject(t, s, "coord")
	ctx := context.Background()
	now := time.Now().UTC()
	orchestrator := sampleRecord("coord")
	orchestrator.Kind = domain.KindOrchestrator

	if _, err := s.ClaimExternalCoordinator(ctx, "coord", "desktop", "Codex Desktop", now, now.Add(5*time.Minute)); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := s.CreateSession(ctx, orchestrator); err == nil {
		t.Fatal("AO orchestrator was inserted while external lease is active")
	}
	if err := s.ClearExternalCoordinator(ctx, "coord", "desktop"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, err := s.CreateSession(ctx, orchestrator); err != nil {
		t.Fatalf("AO orchestrator after clear: %v", err)
	}
	if _, err := s.ClaimExternalCoordinator(ctx, "coord", "desktop", "Codex Desktop", now, now.Add(5*time.Minute)); !errors.Is(err, domain.ErrAOOrchestratorActive) {
		t.Fatalf("external claim with AO orchestrator present: %v", err)
	}
}
