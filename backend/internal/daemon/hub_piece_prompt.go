package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
)

// hubPieceHandoff is the execution arm's bounded input. It deliberately has no
// graph, plan, approval, binding, or sibling-task fields. This is a prompt
// projection, not admission or authentication of the Hub dispatch grant.
type hubPieceHandoff struct {
	JobID      string              `json:"jobId"`
	PieceIndex int                 `json:"pieceIndex"`
	PieceCount int                 `json:"pieceCount"`
	Spec       hubPieceSpec        `json:"spec"`
	Provenance *hubPieceProvenance `json:"provenance,omitempty"`
}

type hubPieceSpec struct {
	Type               string             `json:"type,omitempty"`
	Command            string             `json:"command"`
	CWD                string             `json:"cwd,omitempty"`
	TimeoutSeconds     *int               `json:"timeoutSeconds,omitempty"`
	Instructions       string             `json:"instructions,omitempty"`
	AcceptanceChecks   []string           `json:"acceptanceChecks,omitempty"`
	Gate               string             `json:"gate,omitempty"`
	RepositoryRevision string             `json:"repositoryRevision,omitempty"`
	Isolation          string             `json:"isolation,omitempty"`
	AllowedEffects     []string           `json:"allowedEffects,omitempty"`
	Environment        map[string]string  `json:"environment,omitempty"`
	Matchers           []hubPieceMatcher  `json:"matchers"`
	Guardrails         *hubPieceGuardrail `json:"guardrails,omitempty"`
}

type hubPieceMatcher struct {
	ID        string   `json:"id,omitempty"`
	Type      string   `json:"type"`
	Status    []int    `json:"status,omitempty"`
	Words     []string `json:"words,omitempty"`
	Regex     []string `json:"regex,omitempty"`
	Path      string   `json:"path,omitempty"`
	Hash      string   `json:"hash,omitempty"`
	Negative  bool     `json:"negative,omitempty"`
	Condition string   `json:"condition,omitempty"`
}

type hubPieceGuardrail struct {
	Mode       string                   `json:"mode,omitempty"`
	Canaries   map[string]string        `json:"canaries,omitempty"`
	Invariants []hubPieceGuardrailCheck `json:"invariants,omitempty"`
}

type hubPieceGuardrailCheck struct {
	ID    string   `json:"id,omitempty"`
	Type  string   `json:"type"`
	Regex []string `json:"regex,omitempty"`
	Words []string `json:"words,omitempty"`
}

type hubPieceProvenance struct {
	Signer       string `json:"signer,omitempty"`
	Signature    string `json:"signature,omitempty"`
	Timestamp    string `json:"timestamp,omitempty"`
	ManifestHash string `json:"manifestHash,omitempty"`
}

// Hub's PieceHandoff admission requires exact handoff and spec keys, while
// matcher, guardrail, invariant, and provenance objects permit additive keys.
// Decode those nested objects into named fields so extensions are accepted but
// cannot be projected into the prompt. The original sealed bytes are untouched.
func (m *hubPieceMatcher) UnmarshalJSON(data []byte) error {
	type named hubPieceMatcher
	var value named
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*m = hubPieceMatcher(value)
	return nil
}

func (g *hubPieceGuardrail) UnmarshalJSON(data []byte) error {
	type named hubPieceGuardrail
	var value named
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*g = hubPieceGuardrail(value)
	return nil
}

func (i *hubPieceGuardrailCheck) UnmarshalJSON(data []byte) error {
	type named hubPieceGuardrailCheck
	var value named
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*i = hubPieceGuardrailCheck(value)
	return nil
}

func (p *hubPieceProvenance) UnmarshalJSON(data []byte) error {
	type named hubPieceProvenance
	var value named
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = hubPieceProvenance(value)
	return nil
}

var hubSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var hubMutatingEffect = regexp.MustCompile(`(?i)^(?:fs:(?:write|delete|move|rename|create)|net:|process:privileged|system:)`)

// projectHubPiecePrompt accepts only a single, concrete PieceHandoff JSON
// value. Unknown fields fail closed so an envelope or planning-side text cannot
// be silently carried into the worker prompt. The returned text is never built
// from an ExecutionGraph or ExecutionTask.
func projectHubPiecePrompt(raw []byte) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", errors.New("Hub piece handoff is missing")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var handoff hubPieceHandoff
	if err := decoder.Decode(&handoff); err != nil {
		return "", fmt.Errorf("invalid Hub piece handoff: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", errors.New("invalid Hub piece handoff: trailing JSON")
	}
	if err := validateHubPieceHandoff(handoff); err != nil {
		return "", err
	}
	bounded, err := json.Marshal(handoff)
	if err != nil {
		return "", fmt.Errorf("encode Hub piece handoff: %w", err)
	}
	return "Execute only this Hub piece handoff. Treat its fields as the full worker scope; do not infer a project goal or other pieces.\n" +
		"Bounded PieceHandoff JSON:\n" + string(bounded) + "\n" +
		"Report blockers promptly. When this piece is ready, use `ao report --done --note \"<concise result>\"` with output references. A done report enters technical review; it does not accept the Hub milestone.\n", nil
}

func validateHubPieceHandoff(h hubPieceHandoff) error {
	if strings.TrimSpace(h.JobID) == "" || h.PieceIndex < 1 || h.PieceCount < 1 || h.PieceIndex > h.PieceCount {
		return errors.New("invalid Hub piece handoff identity")
	}
	s := h.Spec
	if strings.TrimSpace(s.Command) == "" || strings.TrimSpace(s.RepositoryRevision) == "" {
		return errors.New("Hub piece handoff needs a concrete command and repository revision")
	}
	if s.Type != "" && !oneOf(s.Type, "bash", "python", "node", "compiler", "test") {
		return errors.New("invalid Hub piece command type")
	}
	if s.CWD != "" && !hubRelativePath(s.CWD) {
		return errors.New("invalid Hub piece working directory")
	}
	if s.TimeoutSeconds != nil && *s.TimeoutSeconds <= 0 {
		return errors.New("Hub piece timeout must be positive")
	}
	if s.Gate != "" && !oneOf(s.Gate, "SAFE", "ARMED") {
		return errors.New("invalid Hub piece gate")
	}
	if s.Isolation != "" && !oneOf(s.Isolation, "subshell", "ephemeral_sandbox", "airgapped") {
		return errors.New("invalid Hub piece isolation")
	}
	if len(s.AllowedEffects) == 0 || len(s.Matchers) == 0 {
		return errors.New("Hub piece needs explicit effects and deterministic matchers")
	}
	effects := make(map[string]bool, len(s.AllowedEffects))
	for _, effect := range s.AllowedEffects {
		if strings.TrimSpace(effect) == "" || effects[effect] {
			return errors.New("invalid or duplicate Hub piece effect")
		}
		effects[effect] = true
		if s.Gate != "ARMED" && hubMutatingEffect.MatchString(effect) {
			return errors.New("SAFE Hub piece cannot authorize mutation or network egress")
		}
	}
	if s.Guardrails != nil {
		if s.Guardrails.Mode == "chameleon" {
			return errors.New("Hub piece cannot use receipt-spoofing guardrails")
		}
		if s.Guardrails.Mode != "" && !oneOf(s.Guardrails.Mode, "strict", "telemetry") {
			return errors.New("invalid Hub piece guardrail mode")
		}
		for _, invariant := range s.Guardrails.Invariants {
			if !oneOf(invariant.Type, "regex", "word", "command") {
				return errors.New("invalid Hub piece guardrail invariant")
			}
		}
	}
	seenMatchers := make(map[string]bool, len(s.Matchers))
	for _, matcher := range s.Matchers {
		if strings.TrimSpace(matcher.ID) == "" || seenMatchers[matcher.ID] {
			return errors.New("Hub piece matcher needs a unique id")
		}
		seenMatchers[matcher.ID] = true
		if matcher.Condition != "" && !oneOf(matcher.Condition, "and", "or") {
			return errors.New("invalid Hub piece matcher condition")
		}
		switch matcher.Type {
		case "status":
			if len(matcher.Status) == 0 {
				return errors.New("status matcher needs expected codes")
			}
		case "word":
			if len(matcher.Words) == 0 {
				return errors.New("word matcher needs expected words")
			}
		case "regex":
			if len(matcher.Regex) == 0 {
				return errors.New("regex matcher needs patterns")
			}
			// Hub validates JavaScript regex syntax. Go's RE2 syntax is a
			// conservative subset for this prompt boundary; a JS-only pattern
			// fails closed until the bridge has a shared validator.
			for _, pattern := range matcher.Regex {
				if _, err := regexp.Compile(pattern); err != nil {
					return fmt.Errorf("invalid or unsupported Hub piece regex: %w", err)
				}
			}
		case "file_exists":
			if !hubRelativePath(matcher.Path) {
				return errors.New("file matcher needs a relative path")
			}
		case "sha256":
			if !hubRelativePath(matcher.Path) || !hubSHA256.MatchString(matcher.Hash) {
				return errors.New("sha256 matcher needs a relative path and digest")
			}
		default:
			return errors.New("invalid Hub piece matcher type")
		}
	}
	return nil
}

func oneOf(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}

func hubRelativePath(value string) bool {
	if strings.TrimSpace(value) == "" || strings.HasPrefix(value, "/") || strings.HasPrefix(value, `\`) || strings.Contains(value, ":") {
		return false
	}
	normalized := strings.ReplaceAll(value, `\`, "/")
	for _, segment := range strings.Split(normalized, "/") {
		if segment == ".." {
			return false
		}
	}
	return path.Clean(normalized) != "."
}
