package domain

import "testing"

const hubGraphFixtureDigest = "34f81de4904f6b0301c890faac4e51ba6326646c6be8240beff073fee7613f2d"

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
			Resources: []string{"src/example.ts"}, ValidationProcedures: []string{"Review source"},
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
