package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
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
			"deliverables":       nonNilStrings(slice.Deliverables),
			"acceptanceCriteria": nonNilStrings(slice.AcceptanceCriteria),
		})
	}
	tasks := make([]any, 0, len(graph.Tasks))
	for _, task := range graph.Tasks {
		tasks = append(tasks, map[string]any{
			"id": task.ID, "sliceId": task.SliceID, "objective": task.Objective,
			"scope": task.Scope, "deliverables": nonNilStrings(task.Deliverables),
			"acceptanceCriteria":      nonNilStrings(task.AcceptanceCriteria),
			"dependencies":            nonNilStrings(task.Dependencies),
			"softDependencies":        nonNilStrings(task.SoftDependencies),
			"priority":                task.Priority,
			"recommendedCapabilities": nonNilStrings(task.RecommendedCapabilities),
			"preferredAgent":          task.PreferredAgent,
			"fallbackAgents":          nonNilStrings(task.FallbackAgents),
			"resources":               nonNilStrings(task.Resources),
			"integrationRequirements": nonNilStrings(task.IntegrationRequirements),
			"validationProcedures":    nonNilStrings(task.ValidationProcedures),
			"complexity":              task.Complexity,
		})
	}
	projection := map[string]any{
		"schemaVersion": graph.SchemaVersion, "id": graph.ID,
		"projectId": string(graph.ProjectID), "objective": graph.Objective,
		"slices": slices, "tasks": tasks,
	}
	if err := validateHubGraphJSONStrings(projection); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(projection); err != nil {
		return "", fmt.Errorf("encode Hub graph contract: %w", err)
	}
	canonical := strings.TrimSuffix(buf.String(), "\n")
	sum := sha256.Sum256([]byte("hub.ao-graph-contract/1\n" + canonical))
	return hex.EncodeToString(sum[:]), nil
}

func validateHubGraphJSONStrings(value any) error {
	switch value := value.(type) {
	case string:
		if !utf8.ValidString(value) {
			return fmt.Errorf("Hub graph contract: invalid UTF-8 is unsupported")
		}
		if strings.ContainsAny(value, "\u2028\u2029") {
			return fmt.Errorf("Hub graph contract: U+2028 and U+2029 are unsupported")
		}
	case []string:
		for _, item := range value {
			if err := validateHubGraphJSONStrings(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := validateHubGraphJSONStrings(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range value {
			if err := validateHubGraphJSONStrings(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func nonNilStrings(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}
