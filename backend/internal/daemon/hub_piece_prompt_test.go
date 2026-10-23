package daemon

import (
	"encoding/json"
	"strings"
	"testing"
)

const validHubPiece = `{
  "jobId":"opaque-job-7",
  "pieceIndex":1,
  "pieceCount":2,
  "spec":{
    "type":"test",
    "command":"node --test tests/piece.test.ts",
    "cwd":"packages/piece",
    "timeoutSeconds":30,
    "instructions":"Run only the named piece and report its receipt.",
    "acceptanceChecks":["focused test passes"],
    "gate":"SAFE",
    "repositoryRevision":"0123456789abcdef0123456789abcdef01234567",
    "isolation":"ephemeral_sandbox",
    "allowedEffects":["fs:read"],
    "matchers":[{"id":"exit-ok","type":"status","status":[0]}]
  }
}`

func TestHubPiecePromptProjectsOnlyBoundedHandoff(t *testing.T) {
	const graphObjective = "FORBIDDEN_GRAPH_OBJECTIVE_4831"
	const siblingTask = "FORBIDDEN_SIBLING_TASK_5926"
	const ownerApproval = "FORBIDDEN_OWNER_APPROVAL_7354"
	// The input has only the one bounded handoff. Neither graph nor sibling task
	// is accepted as an argument to the Hub prompt projection.
	prompt, err := projectHubPiecePrompt([]byte(validHubPiece))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"opaque-job-7", "node --test tests/piece.test.ts", "focused test passes", "fs:read", "ao report --done --note"} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("prompt omitted %q", required)
		}
	}
	for _, forbidden := range []string{graphObjective, siblingTask, ownerApproval, "Project objective:", "Execution graph"} {
		if strings.Contains(prompt, forbidden) {
			t.Fatalf("Hub prompt leaked %q", forbidden)
		}
	}
	var handoff hubPieceHandoff
	if err := json.Unmarshal([]byte(validHubPiece), &handoff); err != nil {
		t.Fatal(err)
	}
	if handoff.Spec.RepositoryRevision == "" {
		t.Fatal("fixture lost pinned revision")
	}
}

func TestHubPiecePromptRejectsPlanningEnvelopeAndExtraTask(t *testing.T) {
	for name, input := range map[string]string{
		"graph objective":      strings.Replace(validHubPiece, `"jobId":"opaque-job-7",`, `"jobId":"opaque-job-7","graphObjective":"FORBIDDEN_GRAPH_OBJECTIVE_4831",`, 1),
		"approval":             strings.Replace(validHubPiece, `"jobId":"opaque-job-7",`, `"jobId":"opaque-job-7","approval":"FORBIDDEN_OWNER_APPROVAL_7354",`, 1),
		"binding envelope":     `{"handoff":` + validHubPiece + `,"binding":{"planHash":"FORBIDDEN_PLAN_BINDING_1482"}}`,
		"sibling task in spec": strings.Replace(validHubPiece, `"type":"test",`, `"type":"test","otherTask":"FORBIDDEN_SIBLING_TASK_5926",`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			prompt, err := projectHubPiecePrompt([]byte(input))
			if err == nil || prompt != "" {
				t.Fatalf("planning input must fail closed: prompt=%q err=%v", prompt, err)
			}
		})
	}
}

func TestHubPiecePromptRejectsMissingAndInvalidHandoff(t *testing.T) {
	for name, input := range map[string]string{
		"missing":                    "",
		"null":                       "null",
		"empty":                      "{}",
		"missing command":            strings.Replace(validHubPiece, `"command":"node --test tests/piece.test.ts"`, `"command":""`, 1),
		"missing revision":           strings.Replace(validHubPiece, `"repositoryRevision":"0123456789abcdef0123456789abcdef01234567"`, `"repositoryRevision":""`, 1),
		"no effects":                 strings.Replace(validHubPiece, `"allowedEffects":["fs:read"]`, `"allowedEffects":[]`, 1),
		"mutating SAFE":              strings.Replace(validHubPiece, `"allowedEffects":["fs:read"]`, `"allowedEffects":["fs:write"]`, 1),
		"missing matchers":           strings.Replace(validHubPiece, `"matchers":[{"id":"exit-ok","type":"status","status":[0]}]`, `"matchers":[]`, 1),
		"invalid matcher condition":  strings.Replace(validHubPiece, `"type":"status","status":[0]`, `"type":"status","status":[0],"condition":"xor"`, 1),
		"receipt spoofing guardrail": strings.Replace(validHubPiece, `"matchers":[{"id":"exit-ok","type":"status","status":[0]}]`, `"matchers":[{"id":"exit-ok","type":"status","status":[0]}],"guardrails":{"mode":"chameleon"}`, 1),
		"upward cwd":                 strings.Replace(validHubPiece, `"cwd":"packages/piece"`, `"cwd":"../other"`, 1),
		"trailing JSON":              validHubPiece + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			prompt, err := projectHubPiecePrompt([]byte(input))
			if err == nil || prompt != "" {
				t.Fatalf("invalid handoff must fail closed: prompt=%q err=%v", prompt, err)
			}
		})
	}
}
