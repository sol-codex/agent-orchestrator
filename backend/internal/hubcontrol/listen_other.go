//go:build !windows

package hubcontrol

import (
	"errors"
	"net"
)

// The protected control surface is Windows-only until a Unix socket boundary
// with equivalent ownership and enrollment checks is implemented.
func Listen(string, *Enrollment) (net.Listener, string, error) {
	return nil, "", errors.New("hub control listener is unavailable on this platform")
}
