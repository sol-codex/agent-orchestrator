package domain

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHubAOEd25519PermitPinnedHubBytesAndLocalIdentity(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	permit, err := ParseHubAOPermit(fixture.Permit)
	if err != nil {
		t.Fatal(err)
	}
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = 0x42
	} // Test-only deterministic vector.
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if hex.EncodeToString(publicKey) != "2152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e069881db12" {
		t.Fatal("test-only Node Ed25519 public-key vector differs")
	}
	trusted := HubAOPermitVerificationContext{
		KeyID: permit.Proof.KeyID, PublicKey: publicKey, HostID: permit.HostID,
		StoreID: permit.StoreID, HostGeneration: permit.HostGeneration, AuthorityEpoch: permit.AuthorityEpoch,
		Prepared: HubAOPreparedReceiptIdentity{
			PrepareID: permit.PrepareID, GraphID: permit.GraphID, ProjectID: permit.ProjectID,
			GraphBytesSHA256: permit.GraphBytesSHA256, ReceiptIdentitySHA256: permit.ReceiptIdentitySHA256,
		},
		Now: time.Date(2026, 10, 23, 15, 1, 30, 0, time.UTC),
	}
	sign := func(value HubAOPermit) []byte {
		t.Helper()
		bytes, err := HubAOPermitSigningBytes(value)
		if err != nil {
			t.Fatal(err)
		}
		value.Proof.Bytes = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(bytes)))
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	bytes, err := HubAOPermitSigningBytes(permit)
	if err != nil || bytes != fixture.ExpectedPermitSigningBytes {
		t.Fatalf("Hub #407 signing bytes differ: err=%v", err)
	}
	raw := sign(permit)
	var signedVector HubAOPermit
	if err := json.Unmarshal(raw, &signedVector); err != nil {
		t.Fatal(err)
	}
	if signedVector.Proof.Bytes != "izsUiPIkA3xxRpiU0vyepSstYirqPqYQn_Sn_2RNoWjRU5bzwmveOXRCA91GQQSCD1mWB8lXuJGqhftmUq3ABg" {
		t.Fatal("test-only Node Ed25519 signature over Hub bytes differs")
	}
	if got, err := VerifyHubAOEd25519Permit(raw, trusted); err != nil || got.GrantID != permit.GrantID {
		t.Fatalf("pinned Ed25519 vector refused: got=%#v err=%v", got, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*HubAOPermit, *HubAOPermitVerificationContext)
	}{
		{"wrong host", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.HostID = "other-host" }},
		{"wrong store", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.StoreID = "other-store" }},
		{"stale generation", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.HostGeneration++ }},
		{"wrong epoch", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.AuthorityEpoch++ }},
		{"wrong prepare", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.PrepareID = "other-prepare" }},
		{"wrong graph", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.GraphID = "other-graph" }},
		{"wrong project", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.ProjectID = "other-project" }},
		{"wrong graph bytes", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.GraphBytesSHA256 = strings.Repeat("9", 64) }},
		{"wrong receipt identity", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) {
			p.ReceiptIdentitySHA256 = strings.Repeat("9", 64)
		}},
		{"mac proof", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.Proof.Scheme = "mac" }},
		{"unknown proof", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.Proof.Scheme = "other" }},
		{"other key ID", func(p *HubAOPermit, _ *HubAOPermitVerificationContext) { p.Proof.KeyID = "other-key" }},
		{"wrong enrolled key", func(_ *HubAOPermit, c *HubAOPermitVerificationContext) {
			c.PublicKey = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)
		}},
		{"missing enrolled key", func(_ *HubAOPermit, c *HubAOPermitVerificationContext) { c.PublicKey = nil }},
		{"future issue", func(_ *HubAOPermit, c *HubAOPermitVerificationContext) {
			c.Now = time.Date(2026, 10, 23, 15, 0, 59, 0, time.UTC)
		}},
		{"expired", func(_ *HubAOPermit, c *HubAOPermitVerificationContext) {
			c.Now = time.Date(2026, 10, 23, 15, 2, 0, 0, time.UTC)
		}},
		{"missing clock", func(_ *HubAOPermit, c *HubAOPermitVerificationContext) { c.Now = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed, local := permit, trusted
			tc.change(&changed, &local)
			if changed.Proof.Scheme == "other" {
				changed.Proof.Bytes = "opaque-proof"
				candidate, err := json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := VerifyHubAOEd25519Permit(candidate, local); err == nil {
					t.Fatal("invalid proof accepted")
				}
				return
			}
			if _, err := VerifyHubAOEd25519Permit(sign(changed), local); err == nil {
				t.Fatal("re-signed wrong identity or trust state accepted")
			}
		})
	}
	for _, proof := range []string{"opaque-proof", "AAAA=", strings.Repeat("A", 86), strings.Repeat("A", 87)} {
		t.Run("invalid signature "+proof[:min(len(proof), 8)], func(t *testing.T) {
			changed := permit
			changed.Proof.Bytes = proof
			candidate, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyHubAOEd25519Permit(candidate, trusted); err == nil {
				t.Fatal("invalid signature accepted")
			}
		})
	}
	var signed HubAOPermit
	if err := json.Unmarshal(raw, &signed); err != nil {
		t.Fatal(err)
	}
	signed.GrantDigest = strings.Repeat("a", 64) // Valid shape, invalid signature.
	changedRaw, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyHubAOEd25519Permit(changedRaw, trusted); err == nil {
		t.Fatal("changed signed identity accepted")
	}
}
