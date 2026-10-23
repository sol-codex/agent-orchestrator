//go:build windows

package hubcontrol

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func currentAccountSID(t *testing.T) string {
	t.Helper()
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	return user.User.Sid.String()
}

func testListen(runFilePath string, e *Enrollment) (net.Listener, string, error) {
	return listenWithAccountLookup(runFilePath, e, func(*windows.SID) (uint32, error) {
		return windows.SidTypeUser, nil
	})
}

func TestWindowsProtectedPipeBindsAndAcceptsOwner(t *testing.T) {
	runFile := t.TempDir() + `\running.json`
	e := testEnrollment()
	e.ServiceSID = currentAccountSID(t)
	ln, name, err := testListen(runFile, e)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := winio.DialPipeAccess(ctx, name, bridgePipeAccess)
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
	enrollment.ServiceSID = currentAccountSID(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var name string
	enabled, err := start(ctx, t.TempDir()+`\running.json`, enrollment, func(runFilePath string, e *Enrollment) (net.Listener, string, error) {
		ln, addr, err := testListen(runFilePath, e)
		name = addr
		return ln, addr, err
	})
	if err != nil || !enabled {
		t.Fatalf("start enabled=%v err=%v", enabled, err)
	}
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return winio.DialPipeAccess(ctx, name, bridgePipeAccess)
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

func TestWindowsPipeRejectsWrongServerIdentityBeforeBind(t *testing.T) {
	e := testEnrollment()
	if e.ServiceSID == currentAccountSID(t) {
		t.Fatal("fixture unexpectedly matches current account")
	}
	ln, name, err := testListen(t.TempDir()+`\running.json`, e)
	if err == nil || ln != nil || name != "" {
		t.Fatalf("wrong identity bound pipe: %q %v", name, err)
	}
}

func TestWindowsPipeSecurityDescriptorUsesOnlyExplicitIdentities(t *testing.T) {
	e := testEnrollment()
	want := "D:P(A;;GA;;;SY)(A;;GA;;;" + e.ServiceSID + ")(A;;0x100003;;;" + e.BridgeSID + ")"
	if got := pipeSecurityDescriptor(e); got != want || bridgePipeAccess&windows.FILE_APPEND_DATA != 0 ||
		strings.Contains(got, ";;;OW") || strings.Contains(got, ";;;WD") || strings.Contains(got, "GRGW") {
		t.Fatalf("unexpected pipe DACL: %q", got)
	}
}

func TestWindowsPipeRejectsGroupAndUnmappedSIDBeforeBind(t *testing.T) {
	e := testEnrollment()
	e.ServiceSID = currentAccountSID(t)
	for _, tc := range []struct {
		name   string
		lookup accountTypeLookup
	}{
		{"custom high RID group", func(sid *windows.SID) (uint32, error) {
			if sid.String() == e.BridgeSID {
				return windows.SidTypeGroup, nil
			}
			return windows.SidTypeUser, nil
		}},
		{"unmapped bridge", func(sid *windows.SID) (uint32, error) {
			if sid.String() == e.BridgeSID {
				return 0, windows.ERROR_NONE_MAPPED
			}
			return windows.SidTypeUser, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, name, err := listenWithAccountLookup(t.TempDir()+`\running.json`, e, tc.lookup)
			if err == nil || ln != nil || name != "" {
				t.Fatalf("invalid account bound pipe: %q %v", name, err)
			}
		})
	}
}

func TestWindowsProductionLookupRejectsUnmappedBridge(t *testing.T) {
	e := testEnrollment()
	e.ServiceSID = currentAccountSID(t)
	serviceSID, err := windows.StringToSid(e.ServiceSID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, accountType, err := serviceSID.LookupAccount("")
	if err != nil || accountType != windows.SidTypeUser {
		t.Fatalf("current process SID did not resolve to a user: type=%d err=%v", accountType, err)
	}
	ln, name, err := Listen(t.TempDir()+`\running.json`, e)
	if !errors.Is(err, ErrInvalidEnrollment) || ln != nil || name != "" {
		t.Fatalf("unmapped bridge bound production pipe: %q %v", name, err)
	}
}
