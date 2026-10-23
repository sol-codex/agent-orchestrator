package execution

import (
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestAgentContractRejectsCapabilitiesAOHasNotMapped(t *testing.T) {
	graph := domain.ExecutionGraph{Tasks: []domain.ExecutionTask{{ID: "t", RecommendedCapabilities: []string{"agent", "harness:codex"}, PreferredAgent: "codex", FallbackAgents: []string{"claude-code"}}}}
	if err := ValidateSupportedAgentContract(graph); err != nil {
		t.Fatal(err)
	}
	graph.Tasks[0].RecommendedCapabilities = []string{"security-audit"}
	if err := ValidateSupportedAgentContract(graph); !errors.Is(err, ErrUnsupportedAgentContract) {
		t.Fatalf("unknown capability error = %v", err)
	}
	graph.Tasks[0].RecommendedCapabilities = nil
	graph.Tasks[0].PreferredAgent = "imaginary-provider"
	if err := ValidateSupportedAgentContract(graph); !errors.Is(err, ErrUnsupportedAgentContract) {
		t.Fatalf("unknown preferred harness error = %v", err)
	}
}
