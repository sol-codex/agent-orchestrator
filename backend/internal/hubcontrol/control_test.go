package hubcontrol

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testEnrollment() *Enrollment {
	e := &Enrollment{StoreID: "store-A", HostID: "host-A", Generation: 7}
	for i := range e.Key {
		e.Key[i] = byte(i + 1)
	}
	return e
}

func request(h http.Handler, e *Enrollment, key string, origin string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	r.Header.Set("X-AO-Store-ID", e.StoreID)
	r.Header.Set("X-AO-Host-ID", e.HostID)
	r.Header.Set("X-AO-Generation", "7")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestStatusRequiresIndependentCredentialAndIdentity(t *testing.T) {
	e := testEnrollment()
	h, err := Handler(e)
	if err != nil {
		t.Fatal(err)
	}
	good := hex.EncodeToString(e.Key[:])
	for _, tc := range []struct {
		name, key, origin string
		want              int
	}{
		{"missing", "", "", http.StatusUnauthorized},
		{"wrong", strings.Repeat("0", 64), "", http.StatusUnauthorized},
		{"malformed", "invalid", "", http.StatusUnauthorized},
		{"browser origin", good, "http://localhost:3000", http.StatusForbidden},
		{"valid", good, "", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := request(h, e, tc.key, tc.origin)
			if w.Code != tc.want {
				t.Fatalf("status %d, want %d", w.Code, tc.want)
			}
			if strings.Contains(w.Body.String(), good) || strings.Contains(w.Body.String(), e.HostID) {
				t.Fatal("response disclosed credential or identity")
			}
		})
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	r.Header.Set("Authorization", "Bearer "+good)
	r.Header.Set("X-AO-Store-ID", "copied-store")
	r.Header.Set("X-AO-Host-ID", e.HostID)
	r.Header.Set("X-AO-Generation", "7")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("copied store status %d", w.Code)
	}
	r.Header.Set("X-AO-Store-ID", e.StoreID)
	r.Header.Set("X-AO-Generation", "6")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("stale generation status %d", w.Code)
	}
	rotated := *e
	rotated.Key[0] ^= 0xff
	rotatedHandler, err := Handler(&rotated)
	if err != nil {
		t.Fatal(err)
	}
	if w := request(rotatedHandler, &rotated, good, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("retired credential status %d", w.Code)
	}
	for _, path := range []string{"/v1/prepare", "/v1/activate", "/v1/graphs"} {
		r := httptest.NewRequest(http.MethodPost, path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Fatalf("mutation path %s status %d", path, w.Code)
		}
	}
}

func TestInvalidEnrollmentAndBindFailureFailClosed(t *testing.T) {
	if _, err := Handler(&Enrollment{}); !errors.Is(err, ErrInvalidEnrollment) {
		t.Fatalf("invalid enrollment: %v", err)
	}
	called := false
	listen := func(string) (net.Listener, string, error) {
		called = true
		return nil, "", errors.New("bind refused")
	}
	enabled, err := start(context.Background(), "ignored", nil, listen)
	if enabled || err != nil || called {
		t.Fatalf("absent enrollment enabled=%v err=%v called=%v", enabled, err, called)
	}
	enabled, err = start(context.Background(), "ignored", &Enrollment{}, listen)
	if enabled || !errors.Is(err, ErrInvalidEnrollment) || called {
		t.Fatalf("invalid enrollment enabled=%v err=%v called=%v", enabled, err, called)
	}
	enabled, err = start(context.Background(), "ignored", testEnrollment(), listen)
	if enabled || err == nil || !called {
		t.Fatalf("bind failure enabled=%v err=%v called=%v", enabled, err, called)
	}
}
