package domain

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

type hubAOWireFixture struct {
	HubContractHead            string          `json:"hubContractHead"`
	ExpectedGrantDigest        string          `json:"expectedGrantDigest"`
	ExpectedPermitSigningBytes string          `json:"expectedPermitSigningBytes"`
	ObservedAt                 string          `json:"observedAt"`
	Grant                      json.RawMessage `json:"grant"`
	Permit                     json.RawMessage `json:"permit"`
	Receipt                    json.RawMessage `json:"receipt"`
	Negative                   []struct {
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
	if fixture.HubContractHead != "b719698dfa6edd7b785adf011935a22df8cdbc96" {
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
	permit, err := ParseHubAOPermit(fixture.Permit)
	if err != nil {
		t.Fatal(err)
	}
	if err := MatchHubAOPermitToGrant(permit, grant, fixture.ObservedAt); err != nil {
		t.Fatal(err)
	}
	signingBytes, err := HubAOPermitSigningBytes(permit)
	if err != nil || signingBytes != fixture.ExpectedPermitSigningBytes {
		t.Fatalf("Hub/Go permit signing bytes differ: err %v, bytes %q", err, signingBytes)
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

func omitHubAOWireField(t *testing.T, raw json.RawMessage, field string) []byte {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	delete(object, field)
	changed, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func TestHubAOWireRequiredPreparedIdentity(t *testing.T) {
	fixture := loadHubAOWireFixture(t)
	for _, field := range []string{"prepareId", "graphBytesSha256", "receiptIdentitySha256", "storeId", "authorityEpoch"} {
		t.Run(field, func(t *testing.T) {
			if _, err := ParseHubAOGrant(omitHubAOWireField(t, fixture.Grant, field)); err == nil {
				t.Fatal("grant omitted required field")
			}
			if _, err := ParseHubAOPermit(omitHubAOWireField(t, fixture.Permit, field)); err == nil {
				t.Fatal("permit omitted required field")
			}
			if _, err := ParseHubAOReceipt(omitHubAOWireField(t, fixture.Receipt, field)); err == nil {
				t.Fatal("receipt omitted required field")
			}
		})
	}
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
		bytes.Replace(fixture.Grant, []byte("grant-1"), []byte(`grant-\ud800`), 1),
		bytes.Replace(fixture.Grant, []byte("grant-1"), []byte(`grant-\udc00`), 1),
		patchHubAOWire(t, fixture.Grant, "grantId", " grant-1"),
		patchHubAOWire(t, fixture.Grant, "hostGeneration", 9007199254740992),
		patchHubAOWire(t, fixture.Grant, "prepareId", "bad/path"),
	} {
		if _, err := ParseHubAOGrant(raw); err == nil {
			t.Fatal("invalid or nonportable grant accepted")
		}
	}
	// Escaping a valid Unicode string preserves the sealed digest.
	grant, err := ParseHubAOGrant(fixture.Grant)
	if err != nil {
		t.Fatal(err)
	}
	grant.GrantID = "grant-é🚀\u2028line<>&"
	grant.RepositoryRevisions[0].Remote = "origin/é🚀\u2029branch"
	grant.GrantDigest, err = HubAOGrantDigest(grant.HubAOGrantPayload)
	if err != nil {
		t.Fatal(err)
	}
	literal, err := json.Marshal(grant)
	if err != nil {
		t.Fatal(err)
	}
	escaped := bytes.ReplaceAll(literal, []byte("é🚀"), []byte(`\u00e9\ud83d\ude80`))
	escaped = bytes.ReplaceAll(escaped, []byte("\u2028"), []byte(`\u2028`))
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
	originalPermit, err := ParseHubAOPermit(fixture.Permit)
	if err != nil {
		t.Fatal(err)
	}
	originalBytes, err := HubAOPermitSigningBytes(originalPermit)
	if err != nil {
		t.Fatal(err)
	}
	alteredBytes, err := HubAOPermitSigningBytes(permit)
	if err != nil || alteredBytes != originalBytes {
		t.Fatalf("opaque proof bytes affected signed identity: %v", err)
	}
	permit.Proof.KeyID = "different-key"
	differentKeyBytes, err := HubAOPermitSigningBytes(permit)
	if err != nil || differentKeyBytes == originalBytes {
		t.Fatalf("proof key identity did not affect signing bytes: %v", err)
	}
	permit.Proof.KeyID = originalPermit.Proof.KeyID
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
