package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// HubGraphContractDigest hashes only the immutable planning fields of an AO
// ExecutionGraph. Runtime state, attempts, reviews, and revision are excluded.
// This is an inert verifier: it does not save a graph or wake the scheduler.
func HubGraphContractDigest(graph ExecutionGraph) (string, error) {
	if err := ValidateExecutionGraph(graph); err != nil {
		return "", err
	}
	slices := make([]any, 0, len(graph.Slices))
	for _, slice := range graph.Slices {
		slices = append(slices, map[string]any{
			"id": slice.ID, "objective": slice.Objective, "scope": slice.Scope,
			"deliverables": nonNilStrings(slice.Deliverables),
			"acceptanceCriteria": nonNilStrings(slice.AcceptanceCriteria),
		})
	}
	tasks := make([]any, 0, len(graph.Tasks))
	for _, task := range graph.Tasks {
		tasks = append(tasks, map[string]any{
			"id": task.ID, "sliceId": task.SliceID, "objective": task.Objective,
			"scope": task.Scope, "deliverables": nonNilStrings(task.Deliverables),
			"acceptanceCriteria": nonNilStrings(task.AcceptanceCriteria),
			"dependencies": nonNilStrings(task.Dependencies),
			"softDependencies": nonNilStrings(task.SoftDependencies),
			"priority": task.Priority,
			"recommendedCapabilities": nonNilStrings(task.RecommendedCapabilities),
			"preferredAgent": task.PreferredAgent,
			"fallbackAgents": nonNilStrings(task.FallbackAgents),
			"resources": nonNilStrings(task.Resources),
			"integrationRequirements": nonNilStrings(task.IntegrationRequirements),
			"validationProcedures": nonNilStrings(task.ValidationProcedures),
			"complexity": task.Complexity,
		})
	}
	projection := map[string]any{
		"schemaVersion": graph.SchemaVersion, "id": graph.ID,
		"projectId": string(graph.ProjectID), "objective": graph.Objective,
		"slices": slices, "tasks": tasks,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(projection); err != nil {
		return "", fmt.Errorf("encode Hub graph contract: %w", err)
	}
	canonical := strings.TrimSuffix(buf.String(), "\n")
	// Go and JavaScript differ on U+2028/U+2029 JSON escaping. Refuse these
	// rare strings until the two codecs share a full canonical JSON standard.
	if strings.Contains(canonical, `\u2028`) || strings.Contains(canonical, `\u2029`) {
		return "", fmt.Errorf("Hub graph contract: U+2028 and U+2029 are unsupported")
	}
	sum := sha256.Sum256([]byte("hub.ao-graph-contract/1\n" + canonical))
	return hex.EncodeToString(sum[:]), nil
}

func nonNilStrings(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}
