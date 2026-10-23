package domain

import "testing"

const hubGraphFixtureDigest = "d569629af2f68ad25a24d5c8a005aa99d98515cccfc50829a2a7f1d5999d2fe4"

func TestHubGraphContractDigestMatchesHubFixture(t *testing.T) {
	graph := ExecutionGraph{
		SchemaVersion: 1, ID: "graph-1", Revision: 1,
		ProjectID: ProjectID("project-hub-001"), Objective: "Approved laptop work",
		Slices: []ExecutionSlice{{
			ID: "slice-1", Objective: "Complete bounded piece", Scope: "One piece",
			Deliverables: []string{"Source change"}, AcceptanceCriteria: []string{"Evidence reviewed"},
		}},
		Tasks: []ExecutionTask{{
			ID: "task-1", SliceID: "slice-1", Objective: "Complete bounded piece",
			Scope: "One piece", Deliverables: []string{"Source change"},
			AcceptanceCriteria: []string{"Evidence reviewed"}, Priority: 1,
			Resources: []string{"src/contracts/investigation.ts"}, ValidationProcedures: []string{"Review source"},
			Complexity: "low", State: TaskPending,
		}},
	}
	digest, err := HubGraphContractDigest(graph)
	if err != nil {
		t.Fatal(err)
	}
	if digest != hubGraphFixtureDigest {
		t.Fatalf("cross-language digest = %s, want %s", digest, hubGraphFixtureDigest)
	}
	graph.Revision = 2
	graph.Tasks[0].Attempts = []TaskAttempt{}
	graph.Tasks[0].Reviews = []TaskReviewRecord{}
	unchanged, err := HubGraphContractDigest(graph)
	if err != nil || unchanged != digest {
		t.Fatalf("runtime fields changed contract digest: %s, %v", unchanged, err)
	}
	graph.Tasks[0].ValidationProcedures = []string{"Different validation"}
	changed, err := HubGraphContractDigest(graph)
	if err != nil || changed == digest {
		t.Fatalf("plan edit did not change contract digest: %s, %v", changed, err)
	}
}

func TestHubGraphContractDigestUnicodeBoundary(t *testing.T) {
	graph := ExecutionGraph{
		SchemaVersion: 1, ID: "graph-1", Revision: 1,
		ProjectID: ProjectID("project-hub-001"), Objective: `Literal \u2028 text`,
		Slices: []ExecutionSlice{{
			ID: "slice-1", Objective: "Piece", Scope: "One piece",
			Deliverables: []string{"Source"}, AcceptanceCriteria: []string{"Reviewed"},
		}},
		Tasks: []ExecutionTask{{
			ID: "task-1", SliceID: "slice-1", Objective: "Piece", Scope: "One piece",
			Deliverables: []string{"Source"}, AcceptanceCriteria: []string{"Reviewed"},
			ValidationProcedures: []string{"Review"}, Complexity: "low", State: TaskPending,
		}},
	}
	if _, err := HubGraphContractDigest(graph); err != nil {
		t.Fatalf("literal escape sequence rejected: %v", err)
	}
	graph.Objective = "Actual \u2028 separator"
	if _, err := HubGraphContractDigest(graph); err == nil {
		t.Fatal("actual U+2028 was accepted")
	}
	graph.Objective = string([]byte{0xff})
	if _, err := HubGraphContractDigest(graph); err == nil {
		t.Fatal("invalid UTF-8 was accepted")
	}
}
