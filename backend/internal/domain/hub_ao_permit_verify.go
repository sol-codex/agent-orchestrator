package domain

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"time"
)

// HubAOPreparedReceiptIdentity must come from a verified AO prepared-row read.
// The caller, not this pure verifier, establishes that provenance.
type HubAOPreparedReceiptIdentity struct {
	PrepareID             string
	GraphID               string
	ProjectID             string
	GraphBytesSHA256      string
	ReceiptIdentitySHA256 string
}

// HubAOPermitVerificationContext is trusted local state supplied by a later
// enrolled control path. It must never be populated from the permit body.
type HubAOPermitVerificationContext struct {
	KeyID          string
	PublicKey      ed25519.PublicKey
	HostID         string
	StoreID        string
	HostGeneration int64
	AuthorityEpoch int64
	Prepared       HubAOPreparedReceiptIdentity
	Now            time.Time
}

// VerifyHubAOEd25519Permit checks the exact Hub wire signing bytes and local
// identity pins. It does not redeem a grant, prove current Hub authority,
// persist a generation, or activate a graph.
func VerifyHubAOEd25519Permit(raw []byte, trusted HubAOPermitVerificationContext) (HubAOPermit, error) {
	refuse := errors.New("Hub AO permit: verification refused")
	permit, err := ParseHubAOPermit(raw)
	if err != nil {
		return HubAOPermit{}, err
	}
	if permit.Proof.Scheme != "signature" || !validHubAOID(trusted.KeyID) ||
		permit.Proof.KeyID != trusted.KeyID || len(trusted.PublicKey) != ed25519.PublicKeySize ||
		!validHubAOID(trusted.HostID) || !validHubAOID(trusted.StoreID) ||
		trusted.HostGeneration <= 0 || trusted.HostGeneration > 9007199254740991 ||
		trusted.AuthorityEpoch < 0 || trusted.AuthorityEpoch > 9007199254740991 ||
		!validHubAOPrepareID(trusted.Prepared.PrepareID) ||
		!validHubAOID(trusted.Prepared.GraphID) || !validHubAOID(trusted.Prepared.ProjectID) ||
		!validLowerHex(trusted.Prepared.GraphBytesSHA256, 64) ||
		!validLowerHex(trusted.Prepared.ReceiptIdentitySHA256, 64) || trusted.Now.IsZero() {
		return HubAOPermit{}, refuse
	}
	if permit.HostID != trusted.HostID || permit.StoreID != trusted.StoreID ||
		permit.HostGeneration != trusted.HostGeneration || permit.AuthorityEpoch != trusted.AuthorityEpoch ||
		permit.PrepareID != trusted.Prepared.PrepareID || permit.GraphID != trusted.Prepared.GraphID ||
		permit.ProjectID != trusted.Prepared.ProjectID ||
		permit.GraphBytesSHA256 != trusted.Prepared.GraphBytesSHA256 ||
		permit.ReceiptIdentitySHA256 != trusted.Prepared.ReceiptIdentitySHA256 {
		return HubAOPermit{}, refuse
	}
	issued, _ := hubAOInstant(permit.IssuedAt)
	useBy, _ := hubAOInstant(permit.UseBy)
	if trusted.Now.Before(issued) || !trusted.Now.Before(useBy) {
		return HubAOPermit{}, refuse
	}
	// A 64-byte Ed25519 signature has exactly 86 unpadded base64url bytes.
	if len(permit.Proof.Bytes) != base64.RawURLEncoding.EncodedLen(ed25519.SignatureSize) {
		return HubAOPermit{}, refuse
	}
	signature, err := base64.RawURLEncoding.DecodeString(permit.Proof.Bytes)
	if err != nil || len(signature) != ed25519.SignatureSize ||
		base64.RawURLEncoding.EncodeToString(signature) != permit.Proof.Bytes {
		return HubAOPermit{}, refuse
	}
	signingBytes, err := HubAOPermitSigningBytes(permit)
	if err != nil || !ed25519.Verify(trusted.PublicKey, []byte(signingBytes), signature) {
		return HubAOPermit{}, refuse
	}
	return permit, nil
}
