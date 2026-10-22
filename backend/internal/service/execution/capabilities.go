package execution

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ErrUnsupportedAgentContract means a graph asks the current AO capacity
// adapter for a capability or harness it cannot represent. Rejecting the plan
// is safer than leaving an apparently READY task silently undispatched.
var ErrUnsupportedAgentContract = errors.New("unsupported execution agent contract")

// ValidateSupportedAgentContract checks the current scheduler vocabulary. AO
// can represent the generic agent role and an exact installed harness identity;
// it has no evidence-backed registry for narrower skills yet.
func ValidateSupportedAgentContract(graph domain.ExecutionGraph) error {
	for _, task := range graph.Tasks {
		for _, capability := range task.RecommendedCapabilities {
			if capability == "agent" {
				continue
			}
			if strings.HasPrefix(capability, "harness:") && domain.AgentHarness(strings.TrimPrefix(capability, "harness:")).IsKnown() {
				continue
			}
			return fmt.Errorf("task %q capability %q: %w", task.ID, capability, ErrUnsupportedAgentContract)
		}
		if task.PreferredAgent != "" && !domain.AgentHarness(task.PreferredAgent).IsKnown() {
			return fmt.Errorf("task %q preferred agent %q: %w", task.ID, task.PreferredAgent, ErrUnsupportedAgentContract)
		}
		for _, agent := range task.FallbackAgents {
			if !domain.AgentHarness(agent).IsKnown() {
				return fmt.Errorf("task %q fallback agent %q: %w", task.ID, agent, ErrUnsupportedAgentContract)
			}
		}
	}
	return nil
}
