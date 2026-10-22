# Execution graph contract (schema version 1)

The `ExecutionGraph` Go type in `backend/internal/domain/execution_graph.go` is
the machine-readable contract for an approved project plan and its execution
record. The daemon imports this contract through its authenticated execution
graph API, stores each revision in SQLite, and reconciles eligible tasks.

## Plan and execution fields

An execution graph has a stable `id`, a `projectId`, a positive `revision`, and
`schemaVersion: 1`. `id` is the plan identity: approval turns this same graph
into the initial execution graph. Replanning increments `revision` and retains
the graph and existing slice/task IDs. Cancel obsolete tasks instead of
silently deleting their history.

Each slice has an `id`, `objective`, `scope`, `deliverables`, and
`acceptanceCriteria`. Each task has an `id`, `sliceId`, `objective`, `scope`,
`deliverables`, `acceptanceCriteria`, `dependencies`, `softDependencies`,
`priority`, `recommendedCapabilities`, `preferredAgent`, `fallbackAgents`,
`resources`, `integrationRequirements`, `validationProcedures`, and
`complexity`. These are plan fields. A task's `state`, `assignment`, and
`attempts` are mutable execution facts. Producers must copy plan fields from
the approved map into the active graph; execution must not invent new IDs.

`dependencies` are hard prerequisites: a task may be `ready` or further along
only when each named task is `completed`. `softDependencies` are advisory
coordination links. They must name existing tasks but do not hold readiness and
may form cycles. Wave or slice order never acts as an implicit dependency.
`resources` are trimmed keys for exclusive editing authority, such as
`backend/internal/domain` or `repo:module:path`. Different ready tasks may
request the same key; the scheduler assigns at most one of them at a time.
Storage normalizes slash spelling and dot segments before claiming a key.
`ValidateExecutionGraph` rejects simultaneous `assigned` or `running` owners
of a key. Persistent, cross-process lease enforcement remains a scheduler and
storage responsibility.

`preferredAgent` and `fallbackAgents` are recommendations, not authorization
to use a provider. The scheduler still checks actual availability, capability,
budget, and user constraints. An empty preferred agent means provider-neutral.
The current daemon accepts only known AO harness IDs in agent preferences and
`agent` or `harness:<known AO harness ID>` capability requirements. An import or
revision with an unsupported requirement receives HTTP 400, so it cannot leave
a permanently unschedulable ready task.
Priority is an integer chosen by the planner; higher numbers are considered
first, but hard dependencies, resource leases, and configured limits take
precedence. Complexity is a planner estimate, not a measured duration.

## Durable task states

| State | Meaning |
| --- | --- |
| `pending` | Planned; prerequisites or a planning decision still prevent dispatch. |
| `ready` | Hard prerequisites are complete; eligible for scheduler checks. |
| `assigned` | A worker/attempt claim exists; asynchronous spawn may still be in progress. |
| `running` | Worker execution is in progress. |
| `blocked` | A known condition needs resolution; independent tasks continue. |
| `review` | Worker output awaits acceptance against the task criteria. |
| `completed` | Accepted and integrated; this state is terminal. |
| `failed` | An attempt or task failed; a bounded retry may return it to ready. |
| `cancelled` | Explicitly retired; this state is terminal. |

The allowed state edges are implemented by `CanTransitionTaskState`. In brief:
`pending` can become `ready`, `blocked`, or `cancelled`; `ready` can become
`assigned`, `blocked`, or `cancelled`; `assigned` can become `running`, return
to `ready`, or become `blocked`, `failed`, or `cancelled`; `running` can become
`review`, `blocked`, `failed`, or `cancelled`; `blocked` can return to `ready`
or `running`, or become `failed` or `cancelled`; `review` can become
`completed`, return to `ready` or `running`, or become `blocked`, `failed`, or
`cancelled`; `failed` can return to `ready` or become `cancelled`.

`assignment` identifies an agent and attempt. Its optional `provider` records
the selected provider for active provider-limit claims across restarts, and
its session ID may be absent during an asynchronous spawn. Once an active
claim has a provider, a later revision cannot silently change it. An active
assignment must reference its unfinished attempt in the same graph snapshot;
storage must create both atomically. `attempts`
retains earlier dispatches and their outcomes for recovery and audit. An
attempt's `retryable` flag defaults to false; the scheduler sets it true only
for a classified transient launch or worker failure. An unfinished attempt
may acquire its session, finish time, error, and retry disposition; a finished
attempt is immutable. A worker reaching the end of a turn does not
automatically move the task to `completed`: review and validation are separate
steps. A task state is a durable execution fact and must not be confused with
AO's derived session display status.

The plan API preserves runtime ownership: `POST /api/v1/execution-graphs`
imports a revision-1 graph without scheduler-owned assignments, attempts, or
terminal results. `PUT /api/v1/execution-graphs/{graphId}` updates planned
fields with the next revision; it cannot manufacture or edit runtime state.
`POST /api/v1/execution-graphs/{graphId}/tasks/{taskId}/review` records an
explicit `accept`, `revise`, or `reject` decision for a current review attempt.
`POST /api/v1/execution-graphs/{graphId}/tasks/{taskId}/unblock` releases a
blocked task only when its hard dependencies are complete, then wakes the
scheduler. `POST /api/v1/execution-graphs/{graphId}/tasks/{taskId}/cancel`
retires an unassigned `pending`, `ready`, `blocked`, or `failed` task while
preserving its identifier and history. Active and review tasks cannot be
cancelled through this endpoint. `POST /api/v1/execution-graphs/{graphId}/reconcile` triggers a
reconciliation pass. Review revision attempts are capped by the scheduler's
configured attempt limit.

## Validation and integration boundary

`ValidateExecutionGraph` checks the schema version, graph/slice/task identity,
required objective and acceptance fields, task state names, dependency
references, duplicate IDs, hard-dependency cycles, readiness against hard
dependencies, and active exclusive-resource conflicts. It checks the internal
shape of one snapshot. `ValidateExecutionGraphTransition` compares consecutive
snapshots. It enforces a one-step revision increase, stable graph/project/slice/
task identities, legal state transitions, append-only attempt history, stable
active resource and assignment claims, and immutability of accepted completed
work. Unfinished task plan fields may change in an explicit revision. These
functions do not write storage, atomically claim resources, prove a session is
alive, or accept worker output. The persistence layer must commit a validated
transition and its leases atomically. The scheduler must reconcile crashed or
lost dispatches against durable session facts before retrying.

Example, shortened to emphasize the execution relationship:

```json
{
  "schemaVersion": 1,
  "id": "next-wave-2026-10-22",
  "revision": 1,
  "projectId": "hub",
  "objective": "Ship the approved next wave",
  "slices": [{
    "id": "slice-e3",
    "objective": "Deliver a usable E3 increment",
    "scope": "Approved E3 source work",
    "deliverables": ["Reviewed source"],
    "acceptanceCriteria": ["Focused checks pass"]
  }],
  "tasks": [{
    "id": "e3-contract",
    "sliceId": "slice-e3",
    "objective": "Define the handoff contract",
    "scope": "Contract only",
    "deliverables": ["Contract"],
    "acceptanceCriteria": ["Owner accepts the contract"],
    "dependencies": [],
    "softDependencies": [],
    "priority": 90,
    "recommendedCapabilities": ["agent"],
    "preferredAgent": "codex",
    "fallbackAgents": [],
    "resources": ["docs/coordination"],
    "integrationRequirements": ["Match existing domain vocabulary"],
    "validationProcedures": ["Review against source"],
    "complexity": "medium",
    "state": "ready",
    "attempts": []
  }]
}
```
