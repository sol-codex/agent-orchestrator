//go:build windows

package hubcontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"path/filepath"

	"github.com/Microsoft/go-winio"
)

// Listen creates a dedicated local control pipe. It is never an HTTP loopback
// or LAN route. The DACL excludes other accounts, while Handler's independent
// credential distinguishes the enrolled bridge from other same-user clients.
func Listen(runFilePath string) (net.Listener, string, error) {
	path, err := filepath.Abs(runFilePath)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256([]byte(filepath.Clean(path)))
	name := `\\.\pipe\ao-hub-control-` + hex.EncodeToString(digest[:8])
	ln, err := winio.ListenPipe(name, &winio.PipeConfig{
		SecurityDescriptor: "D:P(A;;GA;;;SY)(A;;GA;;;OW)",
	})
	if err != nil {
		return nil, "", err
	}
	return ln, name, nil
}
