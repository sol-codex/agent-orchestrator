// Package hubcontrol defines the isolated, currently unarmed Hub control surface.
// No production enrollment is wired yet. In particular, this handler does not
// prepare or activate an execution graph.
package hubcontrol

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Enrollment is supplied only by a future attended enrollment flow. Its key
// must be independent of Hub credentials and dispatch permit proof keys.
type Enrollment struct {
	Key        [32]byte
	StoreID    string
	HostID     string
	Generation uint64
	ServiceSID string // Windows account that runs the AO daemon.
	BridgeSID  string // Windows account allowed to connect to the pipe.
}

var ErrInvalidEnrollment = errors.New("hub control enrollment is invalid")

func validEnrollment(e *Enrollment) bool {
	if e == nil || e.StoreID == "" || e.HostID == "" || e.Generation == 0 ||
		!validAccountSID(e.ServiceSID) || !validAccountSID(e.BridgeSID) || e.ServiceSID == e.BridgeSID {
		return false
	}
	var zero [32]byte
	return subtle.ConstantTimeCompare(e.Key[:], zero[:]) != 1
}

var accountSID = regexp.MustCompile(`^S-1-5-(?:21-(?:[0-9]{1,10}-){3}[0-9]{1,10}|80-(?:[0-9]{1,10}-){4}[0-9]{1,10})$`)

// Accept only explicit account/service identities, never well-known broad groups.
// Windows parses the SID again before the pipe is bound.
func validAccountSID(s string) bool {
	if !accountSID.MatchString(s) {
		return false
	}
	// Domain/local built-in group RIDs are below 1000. This deliberately
	// excludes built-in Administrator too: enrollment requires a dedicated ID.
	if strings.HasPrefix(s, "S-1-5-21-") {
		rid := s[strings.LastIndexByte(s, '-')+1:]
		value, err := strconv.ParseUint(rid, 10, 32)
		return err == nil && value >= 1000
	}
	return true
}

// Handler exposes only an authenticated inert status response. Do not add
// graph mutations here until permit redemption and generation fencing exist.
func Handler(e *Enrollment) (http.Handler, error) {
	if !validEnrollment(e) {
		return nil, ErrInvalidEnrollment
	}
	key := e.Key
	storeID, hostID, generation := e.StoreID, e.HostID, e.Generation
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path != "/v1/status" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		// A browser cannot use this surface, even if it learns the pipe name.
		if r.Header.Get("Origin") != "" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		candidate, err := hex.DecodeString(strings.TrimPrefix(auth, "Bearer "))
		if err != nil || len(candidate) != len(key) || subtle.ConstantTimeCompare(candidate, key[:]) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-AO-Store-ID") != storeID || r.Header.Get("X-AO-Host-ID") != hostID || r.Header.Get("X-AO-Generation") != strconv.FormatUint(generation, 10) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "unarmed"})
	}), nil
}

type listenFunc func(string, *Enrollment) (net.Listener, string, error)

// Start remains disabled when enrollment is absent. There is no credential
// loader or production caller with an enrollment in this source slice.
func Start(ctx context.Context, runFilePath string, enrollment *Enrollment) (bool, error) {
	return start(ctx, runFilePath, enrollment, Listen)
}

func start(ctx context.Context, runFilePath string, enrollment *Enrollment, listen listenFunc) (bool, error) {
	if enrollment == nil {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	handler, err := Handler(enrollment)
	if err != nil {
		return false, err
	}
	ln, _, err := listen(runFilePath, enrollment)
	if err != nil {
		return false, fmt.Errorf("hub control listener unavailable: %w", err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	go func() {
		_ = server.Serve(ln)
	}()
	return true, nil
}
