package domain

import (
	"errors"
	"time"
)

var (
	ErrExternalCoordinatorOwned = errors.New("external coordinator lease belongs to another owner")
	ErrAOOrchestratorActive     = errors.New("AO orchestrator session is active")
	ErrExternalProjectNotFound  = errors.New("project is not active")
)

// ExternalCoordinatorLease records who is coordinating a project outside AO.
// It is separate from an AO orchestrator session and conveys no chat channel.
type ExternalCoordinatorLease struct {
	ProjectID   ProjectID `json:"projectId"`
	OwnerID     string    `json:"ownerId"`
	DisplayName string    `json:"displayName"`
	LastSeenAt  time.Time `json:"lastSeenAt"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

func (l ExternalCoordinatorLease) Active(now time.Time) bool {
	return l.OwnerID != "" && l.ExpiresAt.After(now)
}
