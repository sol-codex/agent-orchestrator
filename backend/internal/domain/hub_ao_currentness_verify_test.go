package domain

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHubAOCurrentnessPinnedHubVectorAndFreshAttempt(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	permit, err := ParseHubAOPermit(fixture.Permit)
	if err != nil {
		t.Fatal(err)
	}
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = 0x42
	}
	privateKey := ed25519.NewKeyFromSeed(seed) // Test-only Hub #414 fixture key.
	trusted := HubAOPermitVerificationContext{
		KeyID: "hub-key-1", PublicKey: privateKey.Public().(ed25519.PublicKey),
		HostID: permit.HostID, StoreID: permit.StoreID, HostGeneration: permit.HostGeneration,
		AuthorityEpoch: permit.AuthorityEpoch,
		Prepared: HubAOPreparedReceiptIdentity{
			PrepareID: permit.PrepareID, GraphID: permit.GraphID, ProjectID: permit.ProjectID,
			GraphBytesSHA256: permit.GraphBytesSHA256, ReceiptIdentitySHA256: permit.ReceiptIdentitySHA256,
		},
		Now: time.Date(2026, 10, 23, 15, 1, 26, 0, time.UTC),
	}
	permit.Proof.Bytes = "izsUiPIkA3xxRpiU0vyepSstYirqPqYQn_Sn_2RNoWjRU5bzwmveOXRCA91GQQSCD1mWB8lXuJGqhftmUq3ABg"
	rawPermit, err := json.Marshal(permit)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := HubAOPermitEnvelopeDigest(permit)
	if err != nil || digest != "4b86b16c8be4aa732a93b2973591994073b8c5692eef0413c34eacc61176f46f" {
		t.Fatalf("Hub #414 full permit digest differs: %q %v", digest, err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(bytesOf(0x24, 32))
	challenge, err := buildHubAOCurrentnessChallenge(permit, "attempt-1", nonce)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewHubAOCurrentnessChallenge(rawPermit, trusted)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHubAOCurrentnessChallenge(rawPermit, trusted)
	if err != nil || first.ActivationAttemptID == second.ActivationAttemptID || first.Nonce == second.Nonce ||
		first.PermitDigest != digest || first.ReceiptIdentitySHA256 != trusted.Prepared.ReceiptIdentitySHA256 {
		t.Fatalf("fresh AO challenge or local identity failed: %v", err)
	}
	attestation := HubAOCurrentnessAttestation{
		HubAOCurrentnessChallenge: challenge, CheckID: "check-1", Allow: true,
		CheckedAt: "2026-10-23T15:01:25.000Z", UseBy: "2026-10-23T15:01:30.000Z",
		Proof: HubAOProof{Scheme: "signature", KeyID: "hub-key-1", Bytes: strings.Repeat("A", 86)},
	}
	signingBytes, err := HubAOCurrentnessSigningBytes(attestation)
	if err != nil || !strings.HasPrefix(signingBytes, hubAOCurrentnessSigningDomain) {
		t.Fatalf("Hub #414 signing bytes refused: %v", err)
	}
	attestation.Proof.Bytes = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signingBytes)))
	if attestation.Proof.Bytes != "jhX35FSo4Bi493JEI6oT9BkMHN0ZwOvprwPtNk3OBTVfa8vU1iZpsd3Skghji70AUyhfLgIQJXHuusbp-T4sBQ" {
		t.Fatal("Hub #414 Ed25519 vector differs")
	}
	rawAttestation, err := json.Marshal(attestation)
	if err != nil {
		t.Fatal(err)
	}
	got, err := VerifyHubAOCurrentnessAttestation(rawAttestation, challenge, rawPermit, trusted, trusted.Now, nil)
	if err != nil || got.CheckID != "check-1" {
		t.Fatalf("pinned Hub #414 currentness vector refused: %v", err)
	}
	if strings.Contains(signingBytes, attestation.Proof.Bytes) {
		t.Fatal("proof bytes were signed recursively")
	}
	for _, tc := range []struct {
		name   string
		change func(*HubAOCurrentnessAttestation, *HubAOCurrentnessChallenge, *HubAOPermitVerificationContext, *time.Time)
	}{
		{"attempt", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.ActivationAttemptID = "attempt-2"
		}},
		{"nonce", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.Nonce = base64.RawURLEncoding.EncodeToString(bytesOf(0x25, 32))
		}},
		{"permit digest", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.PermitDigest = strings.Repeat("9", 64)
		}},
		{"prepare ID", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.PrepareID = "other-prepare"
		}},
		{"receipt identity", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.ReceiptIdentitySHA256 = strings.Repeat("9", 64)
		}},
		{"graph bytes", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.GraphBytesSHA256 = strings.Repeat("9", 64)
		}},
		{"graph contract", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.GraphContractDigest = strings.Repeat("9", 64)
		}},
		{"host", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.HostID = "other-host"
		}},
		{"store", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.StoreID = "other-store"
		}},
		{"generation", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.HostGeneration++
		}},
		{"epoch", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.AuthorityEpoch++
		}},
		{"wrong key", func(_ *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, c *HubAOPermitVerificationContext, _ *time.Time) {
			c.KeyID = "other-key"
		}},
		{"wrong public key", func(_ *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, c *HubAOPermitVerificationContext, _ *time.Time) {
			c.PublicKey = ed25519.NewKeyFromSeed(bytesOf(0x43, 32)).Public().(ed25519.PublicKey)
		}},
		{"missing public key", func(_ *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, c *HubAOPermitVerificationContext, _ *time.Time) {
			c.PublicKey = nil
		}},
		{"changed prepared receipt", func(_ *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, c *HubAOPermitVerificationContext, _ *time.Time) {
			c.Prepared.ReceiptIdentitySHA256 = strings.Repeat("9", 64)
		}},
		{"stale", func(_ *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, now *time.Time) {
			*now = time.Date(2026, 10, 23, 15, 1, 30, 0, time.UTC)
		}},
		{"future", func(_ *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, now *time.Time) {
			*now = time.Date(2026, 10, 23, 15, 1, 24, 0, time.UTC)
		}},
		{"refusal", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.Allow = false
		}},
		{"mac proof", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.Proof.Scheme = "mac"
		}},
		{"wrong signature", func(a *HubAOCurrentnessAttestation, _ *HubAOCurrentnessChallenge, _ *HubAOPermitVerificationContext, _ *time.Time) {
			a.Proof.Bytes = strings.Repeat("A", 86)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed, expected, context, now := attestation, challenge, trusted, trusted.Now
			tc.change(&changed, &expected, &context, &now)
			raw, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyHubAOCurrentnessAttestation(raw, expected, rawPermit, context, now, nil); err == nil {
				t.Fatal("changed currentness identity or freshness accepted")
			}
		})
	}
	if _, err := VerifyHubAOCurrentnessAttestation(rawAttestation, challenge, rawPermit, trusted, trusted.Now, []string{"check-1"}); err == nil {
		t.Fatal("consumed check ID accepted")
	}
	if _, err := VerifyHubAOCurrentnessAttestation(nil, challenge, rawPermit, trusted, trusted.Now, nil); err == nil {
		t.Fatal("missing online attestation accepted")
	}
	changedProof := permit
	changedProof.Proof.Bytes = strings.Repeat("A", 86)
	changedPermit, err := json.Marshal(changedProof)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyHubAOCurrentnessAttestation(rawAttestation, challenge, changedPermit, trusted, trusted.Now, nil); err == nil {
		t.Fatal("changed signed permit accepted")
	}
	for _, raw := range [][]byte{
		[]byte(strings.Replace(string(rawAttestation), `"checkId":"check-1"`, `"checkId":"check-2"`, 1)),
		[]byte(strings.Replace(string(rawAttestation), `"allow":true`, `"allow":false`, 1)),
		[]byte(strings.Replace(string(rawAttestation), `"proof":{`, `"extra":"planning", "proof":{`, 1)),
		[]byte(strings.Replace(string(rawAttestation), `"keyId":"hub-key-1"`, `"KeyId":"hub-key-1"`, 1)),
	} {
		if _, err := VerifyHubAOCurrentnessAttestation(raw, challenge, rawPermit, trusted, trusted.Now, nil); err == nil {
			t.Fatal("tampered or extended attestation accepted")
		}
	}
}

func bytesOf(value byte, size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = value
	}
	return result
}
