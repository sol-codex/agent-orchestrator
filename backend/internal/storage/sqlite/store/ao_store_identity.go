package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

const aoStoreIDPrefix = "ao-store-"

func validAOStoreID(value string) bool {
	if len(value) != len(aoStoreIDPrefix)+64 || !strings.HasPrefix(value, aoStoreIDPrefix) {
		return false
	}
	for _, c := range value[len(aoStoreIDPrefix):] {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func readAOStoreID(ctx context.Context, db gen.DBTX) (string, bool, error) {
	var id string
	err := db.QueryRowContext(ctx, `SELECT store_id FROM ao_store_identity WHERE singleton=1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !validAOStoreID(id) {
		return "", false, errors.New("AO store identity: persisted ID is invalid")
	}
	return id, true, nil
}

// GetAOStoreID is read-only. An absent row remains absent; this method never
// creates an identity during status reads, migrations, or ordinary startup.
func (s *Store) GetAOStoreID(ctx context.Context) (string, bool, error) {
	return readAOStoreID(ctx, s.readDB)
}

// EnsureAOStoreID is an inert, explicit one-time producer for a database-local
// identifier. It is not a machine attestation: copying ao.db copies this ID.
// No production startup or control path calls this method in this slice.
func (s *Store) EnsureAOStoreID(ctx context.Context) (string, error) {
	if id, found, err := s.GetAOStoreID(ctx); err != nil || found {
		return id, err
	}
	if err := s.writeMu.LockContext(ctx); err != nil {
		return "", err
	}
	defer s.writeMu.Unlock()
	if id, found, err := s.GetAOStoreID(ctx); err != nil || found {
		return id, err
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("AO store identity: generate random ID: %w", err)
	}
	candidate := aoStoreIDPrefix + hex.EncodeToString(random[:])
	var id string
	err := s.inTxDB(ctx, "ensure AO store identity", func(_ *gen.Queries, db gen.DBTX) error {
		if _, err := db.ExecContext(ctx, `INSERT INTO ao_store_identity(singleton,store_id,created_at)
			VALUES(1,?,?) ON CONFLICT(singleton) DO NOTHING`, candidate, time.Now().UTC()); err != nil {
			return err
		}
		var found bool
		var err error
		id, found, err = readAOStoreID(ctx, db)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("AO store identity: insert did not persist")
		}
		return nil
	})
	return id, err
}
