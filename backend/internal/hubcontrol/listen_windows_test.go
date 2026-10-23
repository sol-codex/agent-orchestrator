//go:build windows

package hubcontrol

import (
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
