//go:build windows

package hubcontrol

import (
	"context"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
)

func TestWindowsProtectedPipeBindsAndAcceptsOwner(t *testing.T) {
	runFile := t.TempDir() + `\running.json`
	ln, name, err := Listen(runFile)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if !strings.HasPrefix(name, `\\.\pipe\ao-hub-control-`) {
		t.Fatalf("unexpected pipe name %q", name)
	}
	accepted := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
		accepted <- err
	}()
	timeout := 5 * time.Second
	conn, err := winio.DialPipe(name, &timeout)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
}

func TestWindowsHTTPOverProtectedPipeRequiresCredential(t *testing.T) {
	enrollment := testEnrollment()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var name string
	enabled, err := start(ctx, t.TempDir()+`\running.json`, enrollment, func(runFilePath string) (net.Listener, string, error) {
		ln, addr, err := Listen(runFilePath)
		name = addr
		return ln, addr, err
	})
	if err != nil || !enabled {
		t.Fatalf("start enabled=%v err=%v", enabled, err)
	}
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		timeout := 5 * time.Second
		return winio.DialPipe(name, &timeout)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request := func(t *testing.T, path, credential, origin, storeID, generation string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "http://hub-control.local"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		req.Header.Set("X-AO-Store-ID", storeID)
		req.Header.Set("X-AO-Host-ID", enrollment.HostID)
		req.Header.Set("X-AO-Generation", generation)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}
	good := hex.EncodeToString(enrollment.Key[:])
	for _, tc := range []struct {
		name, path, key, origin, storeID, generation string
		want                                         int
	}{
		{"missing", "/v1/status", "", "", enrollment.StoreID, "7", http.StatusUnauthorized},
		{"wrong", "/v1/status", strings.Repeat("0", 64), "", enrollment.StoreID, "7", http.StatusUnauthorized},
		{"browser origin", "/v1/status", good, "http://localhost:3000", enrollment.StoreID, "7", http.StatusForbidden},
		{"wrong store", "/v1/status", good, "", "copied-store", "7", http.StatusForbidden},
		{"stale generation", "/v1/status", good, "", enrollment.StoreID, "6", http.StatusForbidden},
		{"valid", "/v1/status", good, "", enrollment.StoreID, "7", http.StatusOK},
		{"no activation route", "/v1/activate", good, "", enrollment.StoreID, "7", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := request(t, tc.path, tc.key, tc.origin, tc.storeID, tc.generation)
			if status != tc.want {
				t.Fatalf("status %d, want %d: %s", status, tc.want, body)
			}
			if strings.Contains(body, good) || strings.Contains(body, enrollment.HostID) {
				t.Fatal("response disclosed credential or identity")
			}
		})
	}
}
