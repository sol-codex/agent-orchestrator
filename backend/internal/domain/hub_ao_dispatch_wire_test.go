package domain

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type hubAOWireFixture struct {
	HubContractHead     string          `json:"hubContractHead"`
	ExpectedGrantDigest string          `json:"expectedGrantDigest"`
	ObservedAt          string          `json:"observedAt"`
	Grant               json.RawMessage `json:"grant"`
	Permit              json.RawMessage `json:"permit"`
	Receipt             json.RawMessage `json:"receipt"`
	Negative            []struct {
		Name  string `json:"name"`
		Kind  string `json:"kind"`
		Field string `json:"field"`
		Value any    `json:"value"`
	} `json:"negative"`
}

func loadHubAOWireFixture(t *testing.T) hubAOWireFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/hub_ao_dispatch_wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture hubAOWireFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.HubContractHead != "6f5bab472c87080af1b3a27c26dc21c4f7108d3a" {
		t.Fatal("fixture Hub source head drift")
	}
	return fixture
}

func TestHubAOWireParityFixture(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	grant, err := ParseHubAOGrant(fixture.Grant)
	if err != nil {
		t.Fatal(err)
	}
	if grant.GrantDigest != fixture.ExpectedGrantDigest {
		t.Fatalf("Hub/Go canonical digest mismatch: got %s, Hub %s", grant.GrantDigest, fixture.ExpectedGrantDigest)
	}
	calculated, err := HubAOGrantDigest(grant.HubAOGrantPayload)
	if err != nil || calculated != fixture.ExpectedGrantDigest {
		t.Fatalf("Go digest %s, err %v", calculated, err)
	}
	if !strings.Contains(grant.GrantID, "\u2028") || !strings.Contains(grant.RepositoryRevisions[0].Remote, "\u2029") {
		t.Fatal("Unicode parity fixture lost line separators")
	}
	permit, err := ParseHubAOPermit(fixture.Permit)
	if err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOPermitToGrant(permit, grant, fixture.ObservedAt); err != nil {
		t.Fatal(err)
	}
	receipt, err := ParseHubAOReceipt(fixture.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOReceiptToPermit(receipt, permit); err != nil {
		t.Fatal(err)
	}
}

func patchHubAOWire(t *testing.T, raw json.RawMessage, field string, value any) []byte {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object[field] = value
	changed, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func TestHubAOWireNegativeFixture(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	grant, _ := ParseHubAOGrant(fixture.Grant)
	permit, _ := ParseHubAOPermit(fixture.Permit)
	for _, negative := range fixture.Negative {
		t.Run(negative.Name, func(t *testing.T) {
			switch negative.Kind {
			case "grant":
				if _, err := ParseHubAOGrant(patchHubAOWire(t, fixture.Grant, negative.Field, negative.Value)); err == nil {
					t.Fatal("changed sealed grant accepted")
				}
			case "permit":
				changed, err := ParseHubAOPermit(patchHubAOWire(t, fixture.Permit, negative.Field, negative.Value))
				if err == nil {
					err = MatchHubAOPermitToGrant(changed, grant, fixture.ObservedAt)
				}
				if err == nil {
					t.Fatal("changed permit identity accepted")
				}
			case "receipt":
				changed, err := ParseHubAOReceipt(patchHubAOWire(t, fixture.Receipt, negative.Field, negative.Value))
				if err == nil {
					err = MatchHubAOReceiptToPermit(changed, permit)
				}
				if err == nil {
					t.Fatal("changed receipt identity accepted")
				}
			default:
				t.Fatalf("unknown negative case %s", negative.Kind)
			}
		})
	}
}

func TestHubAOWireUnicodeAndStrictShape(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	for _, raw := range [][]byte{
		patchHubAOWire(t, fixture.Grant, "unexpected", "planning text"),
		bytes.Replace(fixture.Grant, []byte("grant-é🚀"), []byte(`grant-\ud800`), 1),
		bytes.Replace(fixture.Grant, []byte("grant-é🚀"), []byte(`grant-\udc00`), 1),
		patchHubAOWire(t, fixture.Grant, "grantId", " grant-1"),
		patchHubAOWire(t, fixture.Grant, "hostGeneration", 9007199254740992),
	} {
		if _, err := ParseHubAOGrant(raw); err == nil {
			t.Fatal("invalid or nonportable grant accepted")
		}
	}
	// A valid surrogate pair, literal Unicode, and JSON's U+2028 escape denote
	// the same string and must reproduce Hub's digest.
	escaped := bytes.Replace(fixture.Grant, []byte("é🚀"), []byte(`\u00e9\ud83d\ude80`), 1)
	escaped = bytes.Replace(escaped, []byte("\u2028"), []byte(`\u2028`), 1)
	if _, err := ParseHubAOGrant(escaped); err != nil {
		t.Fatalf("valid escaped Unicode rejected: %v", err)
	}
}

func TestHubAOWireIdentityDoesNotAuthenticateOrConsume(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	grant, err := ParseHubAOGrant(fixture.Grant)
	if err != nil {
		t.Fatal(err)
	}
	// Opaque proof bytes pass shape and identity comparison. Authentication
	// belongs to a later signed-permit verifier, not this wire parser.
	alteredProof := patchHubAOWire(t, fixture.Permit, "proof", map[string]any{
		"scheme": "signature", "keyId": "hub-key-1", "bytes": "different-opaque-proof",
	})
	permit, err := ParseHubAOPermit(alteredProof)
	if err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOPermitToGrant(permit, grant, fixture.ObservedAt); err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOPermitToGrant(permit, grant, fixture.ObservedAt); err != nil {
		t.Fatal("pure comparison should not imply one-use redemption:", err)
	}
	if err := MatchHubAOPermitToGrant(permit, grant, permit.UseBy); err == nil {
		t.Fatal("expired permit accepted")
	}
	receipt, err := ParseHubAOReceipt(fixture.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOReceiptToPermit(receipt, permit); err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOReceiptToPermit(receipt, permit); err != nil {
		t.Fatal("pure comparison should not imply receipt storage:", err)
	}
}
