# Synthetic execution scheduler timing

`backend/internal/service/execution/performance_test.go` runs a repeatable,
controlled four-task workload against the new execution scheduler and an
intentionally serial reference. This is a scheduling comparison, not a
measurement of the previously installed Agent Orchestrator or of live AI
providers.

## Workload and measurement

- Four independent tasks are ready at the start, with four compatible worker
  slots and a maximum concurrency of four.
- Every synthetic worker has an 80 ms delay before it reports done. The
  scheduler uses an in-memory store, a 2 ms reconciliation cadence, and an
  immediate `accept` decision after review. No real process or provider is
  started.
- The serial reference takes the same four tasks, 80 ms delay, and four-worker
  ceiling, but starts the next task only after the previous one finishes. It
  simulates a coordinator that waits after every dispatch. It has no store or
  validation overhead.
- Each test invocation runs three pairs and reports the median mean dispatch
  delay, median last-task dispatch delay, and median total completion time.
  Delays are measured from the point all four tasks become ready.

Run from `backend`:

```sh
go test ./internal/service/execution -run '^TestSyntheticFourTaskSchedulingPerformance$' -v -count=1
```

Two invocations on Windows with Go 1.27.1 produced these measurements on
2026-10-22. Values are medians of three pairs within each invocation.

| Invocation | Coordinator | Mean ready-to-dispatch | Last ready-to-dispatch | Four-task completion |
| --- | --- | ---: | ---: | ---: |
| 1 | Execution scheduler | 0 ms at clock resolution | 0 ms at clock resolution | 82.5 ms |
| 1 | Serial reference | 120.6 ms | 241.3 ms | 321.6 ms |
| 2 | Execution scheduler | 0 ms at clock resolution | 0 ms at clock resolution | 85.0 ms |
| 2 | Serial reference | 120.9 ms | 242.1 ms | 322.3 ms |

The test checks that all four scheduler tasks reach `completed` and that
exactly four worker starts occurred. It records timing rather than asserting a
speedup threshold, since host scheduling and timer resolution vary. The
reported 0 ms dispatch delays mean the four starts were within this host's
timer resolution; they do not mean dispatch has zero cost.

## Interpretation and limits

This synthetic workload shows that the scheduler can overlap four independent
80 ms waits while the reference deliberately serializes them. It does not
establish a real-world improvement over the old AO workflow. Provider startup,
authentication, API latency, worker variability, review effort, integration
conflicts, cost, failure rates, retries, and restart recovery are absent. The
serial reference also avoids store and validation overhead, so the figures
must not be treated as a controlled comparison of two production releases.
Live evaluation needs equivalent projects, provider limits, and acceptance
checks on both implementations, with those missing metrics recorded separately.
