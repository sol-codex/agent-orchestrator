package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Hub AO dispatch wire parsing checks shape and immutable identity only. It does
// not authenticate a Hub issuer, verify proof bytes, redeem a grant, or activate.
const hubAODispatchVersion = "hub.ao-dispatch/1"
const hubAOGrantDigestDomain = "hub.ao-dispatch-grant/1\n"

type HubAORepositoryRevision struct {
	RepositoryID string `json:"repositoryId"`
	Remote       string `json:"remote"`
	Revision     string `json:"revision"`
}

type HubAOGrantPayload struct {
	ContractVersion           string                    `json:"contractVersion"`
	GrantID                   string                    `json:"grantId"`
	GraphID                   string                    `json:"graphId"`
	ProjectID                 string                    `json:"projectId"`
	PlanID                    string                    `json:"planId"`
	PlanRevision              string                    `json:"planRevision"`
	PlanContentDigest         string                    `json:"planContentDigest"`
	ApprovalID                string                    `json:"approvalId"`
	ApprovalDigest            string                    `json:"approvalDigest"`
	HubAOPlanBindingDigest    string                    `json:"hubAOPlanBindingDigest"`
	GraphContractDigest       string                    `json:"graphContractDigest"`
	HandoffSetDigest          string                    `json:"handoffSetDigest"`
	RepositoryRevisions       []HubAORepositoryRevision `json:"repositoryRevisions"`
	HostID                    string                    `json:"hostId"`
	HostGeneration            int64                     `json:"hostGeneration"`
	AuthorityEpoch            int64                     `json:"authorityEpoch"`
	IssuerOwnerID             string                    `json:"issuerOwnerId"`
	OwnerAuthorizationEventID string                    `json:"ownerAuthorizationEventId"`
	OwnerAuthorizationDigest  string                    `json:"ownerAuthorizationDigest"`
	IssuedAt                  string                    `json:"issuedAt"`
	ExpiresAt                 string                    `json:"expiresAt"`
	OneUse                    bool                      `json:"oneUse"`
}

type HubAOGrant struct {
	HubAOGrantPayload
	GrantDigest string `json:"grantDigest"`
}

type HubAOProof struct {
	Scheme string `json:"scheme"`
	KeyID  string `json:"keyId"`
	Bytes  string `json:"bytes"`
}

type HubAOPermit struct {
	ContractVersion        string     `json:"contractVersion"`
	GrantID                string     `json:"grantId"`
	GrantDigest            string     `json:"grantDigest"`
	RedemptionID           string     `json:"redemptionId"`
	GraphID                string     `json:"graphId"`
	ProjectID              string     `json:"projectId"`
	GraphContractDigest    string     `json:"graphContractDigest"`
	HubAOPlanBindingDigest string     `json:"hubAOPlanBindingDigest"`
	HostID                 string     `json:"hostId"`
	HostGeneration         int64      `json:"hostGeneration"`
	IssuedAt               string     `json:"issuedAt"`
	UseBy                  string     `json:"useBy"`
	Proof                  HubAOProof `json:"proof"`
}

type HubAOReceipt struct {
	ContractVersion        string `json:"contractVersion"`
	GrantID                string `json:"grantId"`
	GrantDigest            string `json:"grantDigest"`
	RedemptionID           string `json:"redemptionId"`
	GraphID                string `json:"graphId"`
	ProjectID              string `json:"projectId"`
	GraphContractDigest    string `json:"graphContractDigest"`
	HubAOPlanBindingDigest string `json:"hubAOPlanBindingDigest"`
	HostID                 string `json:"hostId"`
	HostGeneration         int64  `json:"hostGeneration"`
	ActivationID           string `json:"activationId"`
	GraphRevision          int64  `json:"graphRevision"`
	State                  string `json:"state"`
	ActivatedAt            string `json:"activatedAt"`
}

func strictHubAOJSON(raw []byte, target any) error {
	if !utf8.Valid(raw) {
		return errors.New("Hub AO wire: invalid UTF-8")
	}
	if err := rejectUnpairedJSONSurrogates(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("Hub AO wire: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return errors.New("Hub AO wire: trailing JSON")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("Hub AO wire: %w", err)
	}
	return nil
}

func validHubAOID(value string) bool {
	return value != "" && utf8.ValidString(value) && strings.TrimFunc(value, jsTrimSpace) == value
}

func jsTrimSpace(r rune) bool {
	return r >= 0x09 && r <= 0x0d || r == 0x20 || r == 0xa0 || r == 0x1680 ||
		r >= 0x2000 && r <= 0x200a || r == 0x2028 || r == 0x2029 || r == 0x202f ||
		r == 0x205f || r == 0x3000 || r == 0xfeff
}

func validLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func hubAOInstant(value string) (time.Time, error) {
	if len(value) != len("2006-01-02T15:04:05.000Z") {
		return time.Time{}, errors.New("invalid UTC millisecond instant")
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000Z", value)
	if err != nil || parsed.Format("2006-01-02T15:04:05.000Z") != value {
		return time.Time{}, errors.New("invalid UTC millisecond instant")
	}
	return parsed, nil
}

func (p HubAOGrantPayload) validate() error {
	if p.ContractVersion != hubAODispatchVersion || !p.OneUse ||
		!validHubAOID(p.GrantID) || !validHubAOID(p.GraphID) || !validHubAOID(p.ProjectID) ||
		!validHubAOID(p.PlanID) || !validHubAOID(p.PlanRevision) || !validHubAOID(p.ApprovalID) ||
		!validHubAOID(p.HostID) || !validHubAOID(p.IssuerOwnerID) || !validHubAOID(p.OwnerAuthorizationEventID) ||
		p.OwnerAuthorizationEventID == p.ApprovalID || p.HostGeneration <= 0 || p.HostGeneration > 9007199254740991 ||
		p.AuthorityEpoch < 0 || p.AuthorityEpoch > 9007199254740991 {
		return errors.New("Hub AO grant: invalid identity")
	}
	for _, digest := range []string{p.PlanContentDigest, p.ApprovalDigest, p.HubAOPlanBindingDigest,
		p.GraphContractDigest, p.HandoffSetDigest, p.OwnerAuthorizationDigest} {
		if !validLowerHex(digest, 64) {
			return errors.New("Hub AO grant: invalid digest")
		}
	}
	issued, err := hubAOInstant(p.IssuedAt)
	if err != nil {
		return err
	}
	expires, err := hubAOInstant(p.ExpiresAt)
	if err != nil || !expires.After(issued) {
		return errors.New("Hub AO grant: invalid expiry")
	}
	if len(p.RepositoryRevisions) == 0 {
		return errors.New("Hub AO grant: repository revisions required")
	}
	previous := ""
	for _, repository := range p.RepositoryRevisions {
		if !validHubAOID(repository.RepositoryID) || !validHubAOID(repository.Remote) || !validLowerHex(repository.Revision, 40) ||
			previous != "" && previous >= repository.RepositoryID {
			return errors.New("Hub AO grant: invalid repository revisions")
		}
		previous = repository.RepositoryID
	}
	return nil
}

// HubAOGrantDigest reproduces Hub's domain-separated canonical JSON digest.
// It does not make a grant authoritative.
func HubAOGrantDigest(payload HubAOGrantPayload) (string, error) {
	if err := payload.validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	var canonical bytes.Buffer
	if err := writeHubCanonicalJSON(&canonical, value); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(hubAOGrantDigestDomain + canonical.String()))
	return hex.EncodeToString(sum[:]), nil
}

func writeHubCanonicalJSON(buf *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if value {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case float64:
		if value != float64(int64(value)) {
			return errors.New("Hub AO wire: integer expected")
		}
		buf.WriteString(strconv.FormatInt(int64(value), 10))
	case string:
		writeHubJSONString(buf, value)
	case []any:
		buf.WriteByte('[')
		for i, entry := range value {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeHubCanonicalJSON(buf, entry); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeHubJSONString(buf, key)
			buf.WriteByte(':')
			if err := writeHubCanonicalJSON(buf, value[key]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return errors.New("Hub AO wire: unsupported canonical value")
	}
	return nil
}

func writeHubJSONString(buf *bytes.Buffer, value string) {
	buf.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(buf, "\\u%04x", r)
			} else {
				buf.WriteRune(r)
			}
		}
	}
	buf.WriteByte('"')
}

// Parsing validates payload/digest identity only. No proof or authority lookup occurs.
func ParseHubAOGrant(raw []byte) (HubAOGrant, error) {
	var grant HubAOGrant
	if err := strictHubAOJSON(raw, &grant); err != nil {
		return grant, err
	}
	digest, err := HubAOGrantDigest(grant.HubAOGrantPayload)
	if err != nil {
		return grant, err
	}
	if !validLowerHex(grant.GrantDigest, 64) || grant.GrantDigest != digest {
		return grant, errors.New("Hub AO grant: digest mismatch")
	}
	return grant, nil
}

func ParseHubAOPermit(raw []byte) (HubAOPermit, error) {
	var permit HubAOPermit
	if err := strictHubAOJSON(raw, &permit); err != nil {
		return permit, err
	}
	if permit.ContractVersion != hubAODispatchVersion || !validHubAOID(permit.GrantID) || !validHubAOID(permit.RedemptionID) ||
		!validHubAOID(permit.GraphID) || !validHubAOID(permit.ProjectID) || !validHubAOID(permit.HostID) ||
		!validLowerHex(permit.GrantDigest, 64) || !validLowerHex(permit.GraphContractDigest, 64) ||
		!validLowerHex(permit.HubAOPlanBindingDigest, 64) || permit.HostGeneration <= 0 || permit.HostGeneration > 9007199254740991 ||
		!validHubAOID(permit.Proof.KeyID) || !validHubAOID(permit.Proof.Bytes) ||
		permit.Proof.Scheme != "signature" && permit.Proof.Scheme != "mac" {
		return permit, errors.New("Hub AO permit: invalid shape")
	}
	issued, err := hubAOInstant(permit.IssuedAt)
	if err != nil {
		return permit, err
	}
	useBy, err := hubAOInstant(permit.UseBy)
	if err != nil || !useBy.After(issued) {
		return permit, errors.New("Hub AO permit: invalid expiry")
	}
	return permit, nil
}

func ParseHubAOReceipt(raw []byte) (HubAOReceipt, error) {
	var receipt HubAOReceipt
	if err := strictHubAOJSON(raw, &receipt); err != nil {
		return receipt, err
	}
	if receipt.ContractVersion != hubAODispatchVersion || !validHubAOID(receipt.GrantID) || !validHubAOID(receipt.RedemptionID) ||
		!validHubAOID(receipt.GraphID) || !validHubAOID(receipt.ProjectID) || !validHubAOID(receipt.HostID) || !validHubAOID(receipt.ActivationID) ||
		!validLowerHex(receipt.GrantDigest, 64) || !validLowerHex(receipt.GraphContractDigest, 64) ||
		!validLowerHex(receipt.HubAOPlanBindingDigest, 64) || receipt.HostGeneration <= 0 || receipt.HostGeneration > 9007199254740991 ||
		receipt.GraphRevision <= 0 || receipt.GraphRevision > 9007199254740991 || receipt.State != "active" {
		return receipt, errors.New("Hub AO receipt: invalid shape")
	}
	if _, err := hubAOInstant(receipt.ActivatedAt); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func MatchHubAOPermitToGrant(permit HubAOPermit, grant HubAOGrant, observedAt string) error {
	permitRaw, err := json.Marshal(permit)
	if err != nil {
		return err
	}
	permit, err = ParseHubAOPermit(permitRaw)
	if err != nil {
		return err
	}
	grantRaw, err := json.Marshal(grant)
	if err != nil {
		return err
	}
	grant, err = ParseHubAOGrant(grantRaw)
	if err != nil {
		return err
	}
	at, err := hubAOInstant(observedAt)
	if err != nil {
		return err
	}
	issued, _ := hubAOInstant(permit.IssuedAt)
	useBy, _ := hubAOInstant(permit.UseBy)
	grantIssued, _ := hubAOInstant(grant.IssuedAt)
	grantExpiry, _ := hubAOInstant(grant.ExpiresAt)
	if at.Before(issued) || !at.Before(useBy) || issued.Before(grantIssued) || useBy.After(grantExpiry) {
		return errors.New("Hub AO permit: outside grant or permit interval")
	}
	if permit.GrantID != grant.GrantID || permit.GrantDigest != grant.GrantDigest || permit.GraphID != grant.GraphID ||
		permit.ProjectID != grant.ProjectID || permit.GraphContractDigest != grant.GraphContractDigest ||
		permit.HubAOPlanBindingDigest != grant.HubAOPlanBindingDigest || permit.HostID != grant.HostID ||
		permit.HostGeneration != grant.HostGeneration {
		return errors.New("Hub AO permit: grant identity differs")
	}
	return nil
}

func MatchHubAOReceiptToPermit(receipt HubAOReceipt, permit HubAOPermit) error {
	receiptRaw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	receipt, err = ParseHubAOReceipt(receiptRaw)
	if err != nil {
		return err
	}
	permitRaw, err := json.Marshal(permit)
	if err != nil {
		return err
	}
	permit, err = ParseHubAOPermit(permitRaw)
	if err != nil {
		return err
	}
	activated, _ := hubAOInstant(receipt.ActivatedAt)
	issued, _ := hubAOInstant(permit.IssuedAt)
	useBy, _ := hubAOInstant(permit.UseBy)
	if activated.Before(issued) || !activated.Before(useBy) {
		return errors.New("Hub AO receipt: outside permit interval")
	}
	if receipt.GrantID != permit.GrantID || receipt.GrantDigest != permit.GrantDigest || receipt.RedemptionID != permit.RedemptionID ||
		receipt.GraphID != permit.GraphID || receipt.ProjectID != permit.ProjectID ||
		receipt.GraphContractDigest != permit.GraphContractDigest || receipt.HubAOPlanBindingDigest != permit.HubAOPlanBindingDigest ||
		receipt.HostID != permit.HostID || receipt.HostGeneration != permit.HostGeneration {
		return errors.New("Hub AO receipt: permit identity differs")
	}
	return nil
}

// JSON decoder substitutes U+FFFD for lone surrogate escapes; Hub rejects those.
func rejectUnpairedJSONSurrogates(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' || i+1 >= len(raw) {
			continue
		}
		i++
		if raw[i] != 'u' || i+4 >= len(raw) {
			continue
		}
		code, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			continue
		}
		i += 4
		if code >= 0xdc00 && code <= 0xdfff {
			return errors.New("Hub AO wire: unpaired surrogate")
		}
		if code >= 0xd800 && code <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return errors.New("Hub AO wire: unpaired surrogate")
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return errors.New("Hub AO wire: unpaired surrogate")
			}
			i += 6
		}
	}
	return nil
}
