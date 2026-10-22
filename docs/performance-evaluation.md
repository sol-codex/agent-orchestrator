# Synthetic execution scheduler measurement

`backend/internal/service/execution/performance_test.go` measures a controlled
four-task workload against an intentionally serial reference. It also benchmarks
the scheduler's first READY-frontier pass. These measurements use in-memory
state and synthetic workers; they do not compare two production AO releases.

## Repeat the measurements

From `backend`, with the repository's Go toolchain:

```sh
go test ./internal/service/execution -run 'TestSyntheticTimingMetrics|TestSyntheticFourTaskSchedulingPerformance' -v -count=1
go test ./internal/service/execution -run '^$' -bench '^BenchmarkSyntheticFourReadyTaskTick$' -benchtime=100x -count=3
```

Four independent tasks are ready at the same instant, with four compatible
worker slots and a concurrency limit of four. Each synthetic worker runs for
80 ms. The execution scheduler uses a 2 ms reconciliation cadence and an
immediate `accept` decision after review. The serial reference uses the same
tasks, delay, and four-slot capacity but waits for each worker to finish before
starting the next. Each workload test runs three pairs and reports the median
of each metric. There are no real provider processes or network calls.

- **Ready-to-dispatch delay:** time from the common ready point to each worker
  start. The test reports the mean and latest start across four tasks.
- **Idle slot-time:** four slots multiplied by total completion time, minus the
  sum of each worker's fixed 80 ms busy interval, clipped to completion. It is
  capacity-time, not wall time. **Utilization** is the busy share of that same
  capacity-time.
- **Peak concurrency:** maximum overlap of the four measured busy intervals.
- **Scheduling overhead:** the Go benchmark times only the first `Tick` pass
  over four READY tasks. Graph setup and asynchronous dispatch completion are
  outside its timer. It includes in-memory eligibility, selection, claims,
  and goroutine launch, but excludes SQLite, provider launch, worker work,
  reconciliation, and validation. Use its `ns/op` result; the workload test's
  single-pass `tick_overhead=0s` on Windows is below that clock's resolution,
  not evidence of zero cost.

One Windows amd64 run on 2026-10-22 with Go 1.27.1 and an AMD Ryzen 5 5500U
produced these medians. Timer and host load affect the exact values.

| Workload | Mean ready delay | Latest ready delay | Four-task completion | Idle slot-time | Peak concurrency | Slot utilization |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Execution scheduler | 0.203 ms | 0.405 ms | 82.876 ms | 11.506 ms | 4 | 96.5% |
| Serial reference | 120.659 ms | 241.276 ms | 321.516 ms | 966.063 ms | 1 | 24.9% |

Three fixed 100-iteration `BenchmarkSyntheticFourReadyTaskTick` samples on
the same host were **97,896**, **84,548**, and **86,060 ns/op** (median
**86,060 ns/op**). The test verifies all four scheduler tasks reach
`completed` and exactly four starts occur. A separate deterministic test
checks the idle/concurrency arithmetic. Neither test asserts a speedup
threshold, because host scheduling and timer resolution vary.

## Interpretation and limits

The controlled workload shows concurrent dispatch and synthetic slot use under
the current scheduler. The serial reference deliberately waits after every
dispatch and avoids scheduler store and validation overhead. **A comparable
baseline from the previously installed AO implementation is unavailable**, so
these figures do not establish production improvement. Provider startup,
authentication, API latency, worker variability, review effort, integration
conflicts, cost, failures, retries, and restart recovery are absent. A
production comparison needs equivalent projects, provider limits, acceptance
criteria, and measurements of scheduling delay, worker idle time, completion,
integration failures, and cost on both implementations.
