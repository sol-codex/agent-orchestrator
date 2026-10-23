//go:build windows

package hubcontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// Listen creates a dedicated local control pipe. It is never an HTTP loopback
// or LAN route. The DACL excludes other accounts, while Handler's independent
// credential distinguishes the enrolled bridge from other permitted clients.
func Listen(runFilePath string, enrollment *Enrollment) (net.Listener, string, error) {
	return listenWithAccountLookup(runFilePath, enrollment, func(sid *windows.SID) (uint32, error) {
		_, _, accountType, err := sid.LookupAccount("")
		return accountType, err
	})
}

type accountTypeLookup func(*windows.SID) (uint32, error)

const bridgePipeAccess = windows.FILE_READ_DATA | windows.FILE_WRITE_DATA | windows.SYNCHRONIZE

func listenWithAccountLookup(runFilePath string, enrollment *Enrollment, lookup accountTypeLookup) (net.Listener, string, error) {
	if !validEnrollment(enrollment) {
		return nil, "", ErrInvalidEnrollment
	}
	serviceSID, err := windows.StringToSid(enrollment.ServiceSID)
	if err != nil || serviceSID.String() != enrollment.ServiceSID {
		return nil, "", ErrInvalidEnrollment
	}
	bridgeSID, err := windows.StringToSid(enrollment.BridgeSID)
	if err != nil || bridgeSID.String() != enrollment.BridgeSID {
		return nil, "", ErrInvalidEnrollment
	}
	for _, sid := range []*windows.SID{serviceSID, bridgeSID} {
		accountType, err := lookup(sid)
		if err != nil || accountType != windows.SidTypeUser {
			return nil, "", ErrInvalidEnrollment
		}
	}
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return nil, "", fmt.Errorf("hub control process identity unavailable: %w", err)
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil || user.User.Sid.String() != enrollment.ServiceSID {
		return nil, "", fmt.Errorf("hub control process identity mismatch")
	}
	path, err := filepath.Abs(runFilePath)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256([]byte(filepath.Clean(path)))
	name := `\\.\pipe\ao-hub-control-` + hex.EncodeToString(digest[:8])
	ln, err := winio.ListenPipe(name, &winio.PipeConfig{
		SecurityDescriptor: pipeSecurityDescriptor(enrollment),
	})
	if err != nil {
		return nil, "", err
	}
	return ln, name, nil
}

func pipeSecurityDescriptor(e *Enrollment) string {
	// Generic write contains FILE_APPEND_DATA, which also grants the right to
	// create a server pipe instance. The bridge receives only client data and
	// synchronization rights; its dialer must request this exact access mask.
	return fmt.Sprintf("D:P(A;;GA;;;SY)(A;;GA;;;%s)(A;;0x%X;;;%s)", e.ServiceSID, bridgePipeAccess, e.BridgeSID)
}
