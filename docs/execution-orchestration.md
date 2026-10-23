# Execution-aware orchestration in the Marsh fork

## Architecture assessment and cause

AO already registers multiple agent harnesses, can run multiple worker sessions at once, and returns from worker creation without waiting for a worker's whole task. The agent readiness catalog supplies observed installation and authentication state. Sessions, worker reports, PR facts, and recurring automation runs persist in SQLite. Existing automation dispatch has transaction claims and restart reconciliation, but its rows represent scheduled occurrences, not a project plan.

Before this change, AO had no durable project task graph, dependency edges, task-to-worker assignment ledger, exclusive resource claims, or event that advanced newly eligible work. The board derives its orchestrator label from AO orchestrator session rows. Codex Desktop coordinating outside AO therefore appeared as “no orchestrator,” even while it was coordinating. Worker `done` reports are claims and route to an active AO orchestrator; they cannot by themselves establish acceptance.

This explains the observed serialization: the worker launch mechanism was concurrent, but the coordinator had to remember the remaining frontier in conversational context and choose when to dispatch again. Waiting for one worker was a coordinator decision, not a blocking worker API requirement. The existing daemon, session manager, readiness catalog, report store, and SQLite transaction pattern are reused.

| Layer | Responsibility in this design |
| --- | --- |
| Codex instruction | Invoke `ao-orchestration` for substantial plans and execution; scan the READY frontier before waiting. |
| Reusable skill | Preserve the approved graph, define task contracts, select agents, review outcomes, and replan locally. |
| AO application | Persist graphs and attempts, claim resources and capacity atomically, spawn idempotent workers, reconcile events and restarts, expose review actions. |

Prompt instructions alone cannot supply atomic claims or recover a lost process. The app scheduler handles those facts; Codex makes architectural and acceptance judgments.

## Execution graph

`ExecutionGraph` schema version 1 holds a stable graph ID, project ID, revision, objective, slices, and tasks. A task records objective, scope, deliverables, acceptance criteria, hard and advisory dependencies, priority, recommended capabilities, preferred and fallback agents, exclusive resources, integration requirements, validation procedures, complexity, state, assignment, and attempt history. The approved plan is imported at revision 1. Later revisions preserve task IDs, attempts, and accepted completed work. A changed requirement becomes a new revision or task, rather than a rebuilt plan with unrelated IDs.

States are `pending`, `ready`, `assigned`, `running`, `blocked`, `review`, `completed`, `failed`, and `cancelled`. Hard dependencies must be completed before READY. Advisory dependencies do not block. Waves explain a plan to people; they are not barriers in the scheduler. Higher priority and downstream unlock count order eligible candidates. The store rechecks eligibility and capacity inside the claim transaction, so stale capacity snapshots do not create two owners.

At a claim, the store records the task assignment, attempt, provider, and project-scoped exclusive resource keys in one transaction. It checks global active worker and provider limits. A lease deadline is an observation point, not permission to start a duplicate worker: the scheduler first looks up the existing attempt's session using its stable client request ID. Only a fenced task transition releases a resource claim. Reconciliation runs at startup, on explicit wake, and periodically if an event is missed.

The worker prompt carries the task contract and asks for an AO `--done` report with output references. A new report wakes the scheduler; periodic reconciliation covers missed events. A done report moves the task to REVIEW. The coordinator or another designated reviewer checks the deliverables and records `accept`, `revise`, or `reject`; only `accept` completes the task and unlocks dependents. A worker exit without a done report is a failed attempt. Failed attempts retry only when marked retryable and below the configured attempt cap. A failed inspection or uncertain launch retains its assignment and resource claim for investigation; absence of a session row alone does not prove it is safe to retry.

Each explicit review appends an immutable `reviews[]` record to the task's graph snapshot in the same transaction that finishes the fenced attempt. The record holds `attemptId`, `decision`, and `reviewedAt`; the review request can also provide `sourceHead` (full Git object ID), `prUrl` (HTTPS URL), a short `validationSummary`, and up to 20 `evidenceRefs`. The latter are references, not raw test logs. Existing decision-only review requests remain valid and produce a record without source evidence. Graph import and plan revisions cannot inject, erase, or rewrite review history. The graph GET response returns the ledger after daemon restart. A stale attempt cannot append a decision. This is a coordinator-supplied evidence record; AO does not independently verify the PR, Git object, tests, or merge state. The ledger applies to future review transitions only. Completed tasks remain immutable; earlier Hub review evidence stays in the corresponding PRs (#394 and #395) and AO worker reports linked by their attempt session IDs.

## Agent selection and limits

The current adapter uses AO's cached readiness observations and the graph's explicit preferred/fallback harness order. It admits only installed harnesses with confirmed `ready` status; an unknown observation waits for a later refresh. It does not invent provider quality scores. The supported capability vocabulary is `agent` and `harness:<id>` for a known AO harness; unsupported labels are rejected at import rather than silently leaving a task READY. Plans requiring narrower skills must record their agent choice explicitly until a supported capability registry exists. The scheduler limits concurrent active attempts to four and supports per-provider limits. It tracks `BudgetBlocked` if a capacity source supplies it; cost budget enforcement is not yet connected to AO usage policy. The daemon accepts only an imported graph, so no work is fabricated to fill idle capacity.

## External Codex Desktop coordinator

The separate project-scoped external coordinator registration records a named owner and short lease. The board can then show Codex Desktop as coordinating while the lease is live. This registration is a status claim: it does not create a fake AO orchestrator session or redirect AO worker reports into this Codex chat. The graph scheduler reads durable report rows directly, and the coordinator checks the execution graph through the API. If an AO orchestrator session is active, that session remains the authoritative AO session and the external claim conflicts. The Codex Desktop coordinator renews its lease while working and clears it on handoff.

## Operating sequence

1. Build a human-readable plan and schema-v1 graph with matching IDs and acceptance criteria. Review the graph before import.
2. Import revision 1 through the execution-graph API. A valid import wakes the scheduler. Assignments start only for ready tasks with compatible capacity and free resources.
3. Follow running sessions and persisted reports. A done report becomes REVIEW. Inspect actual artifacts, tests, exact source head, and integration compatibility before recording a review decision.
4. On failure or a user change, revise the graph at its next revision. Preserve accepted work and existing attempt IDs. Reconcile immediately; periodic reconciliation catches a missed event.
5. Before replacing the AO daemon, stop the old process cleanly and preserve its data directory. Test the new binary against a separate scratch data directory first, then restart the configured process. A failed launch or uncertain session observation must not trigger duplicate work.

## Validation and remaining work

Focused domain tests cover graph integrity and continuity. Scheduler tests cover parallel dispatch, hard dependency order, rolling unlocks, failure isolation, idle waiting, restart adoption, resource/provider limits, validation, missed events, and critical-path ordering. SQLite tests check atomic claims and resource isolation. API and adapter tests check graph import, review, report/exit distinction, and idempotent session identity. Full package and scratch-daemon results, plus measured performance data, belong in the release evidence; source tests alone do not prove a live provider used the new path. The review ledger stores a decision and optional evidence references, but does not yet store reviewer identity, per-criterion verification, or independent artifact verification. An `accept` decision remains a coordinator assertion until those checks are carried out and referenced.

Operational follow-ups: wire cost budgets and an evidence-backed capability/performance registry if those inputs become available; measure equivalent real workloads before claiming faster project completion; add cross-process graph CDC only if periodic reconciliation and direct wakes are insufficient.
