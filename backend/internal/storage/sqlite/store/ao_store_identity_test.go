package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

func openAOStoreIdentityFixture(t *testing.T, path string, migrate bool) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if migrate {
		migration, err := os.ReadFile("../migrations/20261023204046574_ao_store_identity.sql")
		if err != nil {
			t.Fatal(err)
		}
		up, _, ok := strings.Cut(string(migration), "-- +goose Down")
		if !ok {
			t.Fatal("store identity migration missing Down boundary")
		}
		if _, err := db.Exec(up); err != nil {
			t.Fatalf("store identity migration: %v", err)
		}
	}
	return NewStore(db, db), db
}

func TestAOStoreIdentityIsExplicitDurableAndCopyable(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	s, db := openAOStoreIdentityFixture(t, sourcePath, true)
	if id, found, err := s.GetAOStoreID(ctx); err != nil || found || id != "" {
		t.Fatalf("read-only getter minted identity: id=%q found=%v err=%v", id, found, err)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM ao_store_identity`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("read-only getter wrote row: rows=%d err=%v", rows, err)
	}
	id, err := s.EnsureAOStoreID(ctx)
	if err != nil || !validAOStoreID(id) {
		t.Fatalf("ensure ID=%q err=%v", id, err)
	}
	if again, err := s.EnsureAOStoreID(ctx); err != nil || again != id {
		t.Fatalf("second ensure changed ID: %q -> %q err=%v", id, again, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedDB := openAOStoreIdentityFixture(t, sourcePath, false)
	if got, found, err := reopened.GetAOStoreID(ctx); err != nil || !found || got != id {
		t.Fatalf("reopen changed ID: got=%q found=%v err=%v", got, found, err)
	}
	if err := reopenedDB.Close(); err != nil {
		t.Fatal(err)
	}
	// An AO_DATA_DIR database copy carries the same ID. It is not host proof.
	copyPath := filepath.Join(t.TempDir(), "copied.db")
	bytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(copyPath, bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	copied, _ := openAOStoreIdentityFixture(t, copyPath, false)
	if got, found, err := copied.GetAOStoreID(ctx); err != nil || !found || got != id {
		t.Fatalf("copy changed ID: got=%q found=%v err=%v", got, found, err)
	}
	fresh, _ := openAOStoreIdentityFixture(t, filepath.Join(t.TempDir(), "fresh.db"), true)
	if distinct, err := fresh.EnsureAOStoreID(ctx); err != nil || distinct == id {
		t.Fatalf("fresh database reused ID: distinct=%q err=%v", distinct, err)
	}
}

func TestAOStoreIdentityConcurrentFirstEnsure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	first, db := openAOStoreIdentityFixture(t, path, true)
	second, _ := openAOStoreIdentityFixture(t, path, false)
	var wg sync.WaitGroup
	results := make(chan string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := first
			if i%2 == 1 {
				store = second
			}
			id, err := store.EnsureAOStoreID(context.Background())
			if err != nil {
				t.Errorf("concurrent ensure: %v", err)
			}
			results <- id
		}(i)
	}
	wg.Wait()
	close(results)
	var expected string
	for id := range results {
		if expected == "" {
			expected = id
		}
		if id != expected || !validAOStoreID(id) {
			t.Fatalf("concurrent IDs differed: first=%q got=%q", expected, id)
		}
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM ao_store_identity`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("concurrent ensure wrote %d rows: %v", rows, err)
	}
}

func TestAOStoreIdentityRejectsMalformedAndImmutableRow(t *testing.T) {
	s, db := openAOStoreIdentityFixture(t, filepath.Join(t.TempDir(), "malformed.db"), true)
	malformed := aoStoreIDPrefix + strings.Repeat("z", 64) // SQL bounds allow it; Go must refuse.
	if _, err := db.Exec(`INSERT INTO ao_store_identity(singleton,store_id,created_at) VALUES(1,?,CURRENT_TIMESTAMP)`, malformed); err != nil {
		t.Fatal(err)
	}
	if id, found, err := s.GetAOStoreID(context.Background()); err == nil || found || id != "" {
		t.Fatalf("malformed persisted ID read as valid: id=%q found=%v err=%v", id, found, err)
	}
	if id, err := s.EnsureAOStoreID(context.Background()); err == nil || id != "" {
		t.Fatalf("malformed persisted ID silently rotated: id=%q err=%v", id, err)
	}
	if _, err := db.Exec(`UPDATE ao_store_identity SET store_id=? WHERE singleton=1`, aoStoreIDPrefix+strings.Repeat("a", 64)); err == nil {
		t.Fatal("persisted store ID was mutable")
	}
	if _, err := db.Exec(`DELETE FROM ao_store_identity WHERE singleton=1`); err == nil {
		t.Fatal("persisted store ID was deletable")
	}
}
