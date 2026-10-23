package execution

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// This workload measures the scheduler's timing against an intentionally serial
// coordinator under the same four-task, four-worker ceiling. It is synthetic:
// workers merely wait, and neither implementation launches a real provider.
const (
	syntheticTasks     = 4
	syntheticWorkDelay = 80 * time.Millisecond
	syntheticRuns      = 3
)

type syntheticDispatcher struct {
	mu        sync.Mutex
	started   map[domain.SessionID]time.Time
	byAttempt map[string]domain.SessionID
}

func newSyntheticDispatcher() *syntheticDispatcher {
	return &syntheticDispatcher{started: make(map[domain.SessionID]time.Time), byAttempt: make(map[string]domain.SessionID)}
}

func (d *syntheticDispatcher) Spawn(_ context.Context, _ domain.ExecutionGraph, _ domain.ExecutionTask, _ Agent, attemptID string) (domain.SessionID, error) {
	id := domain.SessionID("synthetic-" + attemptID)
	d.mu.Lock()
	d.started[id] = time.Now()
	d.byAttempt[attemptID] = id
	d.mu.Unlock()
	return id, nil
}

func (d *syntheticDispatcher) Lookup(_ context.Context, attemptID string) (domain.SessionID, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id, found := d.byAttempt[attemptID]
	return id, found, nil
}

func (d *syntheticDispatcher) Inspect(_ context.Context, id domain.SessionID) (WorkerObservation, error) {
	d.mu.Lock()
	started, found := d.started[id]
	d.mu.Unlock()
	if !found {
		return WorkerObservation{State: WorkerUnknown}, nil
	}
	if time.Since(started) >= syntheticWorkDelay {
		return WorkerObservation{State: WorkerDone}, nil
	}
	return WorkerObservation{State: WorkerRunning}, nil
}

func (d *syntheticDispatcher) starts() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]time.Time, 0, len(d.started))
	for _, started := range d.started {
		result = append(result, started)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Before(result[j]) })
	return result
}

type syntheticTiming struct {
	meanDispatch       time.Duration
	lastDispatch       time.Duration
	completion         time.Duration
	idleSlotTime       time.Duration
	peakConcurrency    int
	utilization        float64
	schedulingOverhead time.Duration
}

// Idle slot-time counts unused capacity across the entire run, not just wall
// time with every worker idle. Worker busy intervals have the same fixed delay
// in both workloads; inspection/acceptance time remains available-slot time.
func syntheticTimingFromStarts(starts []time.Time, readyAt, completedAt time.Time, schedulingOverhead time.Duration) syntheticTiming {
	ordered := append([]time.Time(nil), starts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Before(ordered[j]) })
	completion := completedAt.Sub(readyAt)
	var dispatchTotal, busy time.Duration
	peak := 0
	for _, started := range ordered {
		dispatchTotal += started.Sub(readyAt)
		end := started.Add(syntheticWorkDelay)
		if end.After(completedAt) {
			end = completedAt
		}
		if end.After(started) {
			busy += end.Sub(started)
		}
		concurrent := 0
		for _, other := range ordered {
			if !other.After(started) && other.Add(syntheticWorkDelay).After(started) {
				concurrent++
			}
		}
		if concurrent > peak {
			peak = concurrent
		}
	}
	capacity := time.Duration(syntheticTasks) * completion
	idle := capacity - busy
	if idle < 0 {
		idle = 0
	}
	var utilization float64
	if capacity > 0 {
		utilization = 100 * float64(busy) / float64(capacity)
	}
	return syntheticTiming{
		meanDispatch: dispatchTotal / time.Duration(len(ordered)), lastDispatch: ordered[len(ordered)-1].Sub(readyAt),
		completion: completion, idleSlotTime: idle, peakConcurrency: peak,
		utilization: utilization, schedulingOverhead: schedulingOverhead,
	}
}

func syntheticSchedulerRun(t *testing.T) syntheticTiming {
	t.Helper()
	store := newMemoryStore(graph(
		task("synthetic-a", domain.TaskReady), task("synthetic-b", domain.TaskReady),
		task("synthetic-c", domain.TaskReady), task("synthetic-d", domain.TaskReady),
	))
	dispatcher := newSyntheticDispatcher()
	sequence := 0
	svc, err := New(store, dispatcher, agents(syntheticTasks), staticValidator{decision: ReviewAccept}, Config{
		MaxConcurrent: syntheticTasks, ReconcileInterval: 2 * time.Millisecond,
		NewAttemptID: func() string { sequence++; return fmt.Sprintf("synthetic-attempt-%d", sequence) },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	readyAt := time.Now()
	tickStart := time.Now()
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	schedulingOverhead := time.Since(tickStart)
	if got := len(store.claimIDs()); got != syntheticTasks {
		t.Fatalf("initial Tick claimed %d tasks, want %d", got, syntheticTasks)
	}
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx, nil) }()
	for {
		allComplete := true
		for _, id := range []string{"synthetic-a", "synthetic-b", "synthetic-c", "synthetic-d"} {
			if store.state("g", id) != domain.TaskCompleted {
				allComplete = false
				break
			}
		}
		if allComplete {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("synthetic scheduler did not complete all four tasks")
		}
		time.Sleep(time.Millisecond)
	}
	completedAt := time.Now()
	cancel()
	<-done
	starts := dispatcher.starts()
	if len(starts) != syntheticTasks {
		t.Fatalf("synthetic scheduler dispatched %d tasks, want %d", len(starts), syntheticTasks)
	}
	return syntheticTimingFromStarts(starts, readyAt, completedAt, schedulingOverhead)
}

func syntheticSerialRun() syntheticTiming {
	readyAt := time.Now()
	starts := make([]time.Time, 0, syntheticTasks)
	for i := 0; i < syntheticTasks; i++ {
		starts = append(starts, time.Now())
		time.Sleep(syntheticWorkDelay)
	}
	return syntheticTimingFromStarts(starts, readyAt, time.Now(), 0)
}

func medianSyntheticTiming(samples []syntheticTiming) syntheticTiming {
	median := func(selectDuration func(syntheticTiming) time.Duration) time.Duration {
		values := make([]time.Duration, len(samples))
		for i, sample := range samples {
			values[i] = selectDuration(sample)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return values[len(values)/2]
	}
	return syntheticTiming{
		meanDispatch:       median(func(sample syntheticTiming) time.Duration { return sample.meanDispatch }),
		lastDispatch:       median(func(sample syntheticTiming) time.Duration { return sample.lastDispatch }),
		completion:         median(func(sample syntheticTiming) time.Duration { return sample.completion }),
		idleSlotTime:       median(func(sample syntheticTiming) time.Duration { return sample.idleSlotTime }),
		schedulingOverhead: median(func(sample syntheticTiming) time.Duration { return sample.schedulingOverhead }),
		peakConcurrency: func() int {
			values := make([]int, len(samples))
			for i, sample := range samples {
				values[i] = sample.peakConcurrency
			}
			sort.Ints(values)
			return values[len(values)/2]
		}(),
		utilization: func() float64 {
			values := make([]float64, len(samples))
			for i, sample := range samples {
				values[i] = sample.utilization
			}
			sort.Float64s(values)
			return values[len(values)/2]
		}(),
	}
}

func TestSyntheticTimingMetrics(t *testing.T) {
	readyAt := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	parallel := syntheticTimingFromStarts([]time.Time{readyAt, readyAt, readyAt, readyAt}, readyAt, readyAt.Add(82*time.Millisecond), time.Millisecond)
	if parallel.idleSlotTime != 8*time.Millisecond || parallel.peakConcurrency != 4 || parallel.schedulingOverhead != time.Millisecond {
		t.Fatalf("parallel metrics = %+v", parallel)
	}
	serialStarts := make([]time.Time, syntheticTasks)
	for i := range serialStarts {
		serialStarts[i] = readyAt.Add(time.Duration(i) * syntheticWorkDelay)
	}
	serial := syntheticTimingFromStarts(serialStarts, readyAt, readyAt.Add(4*syntheticWorkDelay), 0)
	if serial.idleSlotTime != 960*time.Millisecond || serial.peakConcurrency != 1 || serial.utilization != 25 {
		t.Fatalf("serial metrics = %+v", serial)
	}
}

func TestSyntheticFourTaskSchedulingPerformance(t *testing.T) {
	var scheduled, serial []syntheticTiming
	for i := 0; i < syntheticRuns; i++ {
		scheduled = append(scheduled, syntheticSchedulerRun(t))
		serial = append(serial, syntheticSerialRun())
	}
	schedulerMedian := medianSyntheticTiming(scheduled)
	serialMedian := medianSyntheticTiming(serial)
	t.Logf("synthetic workload: tasks=%d worker_limit=%d worker_delay=%s runs=%d; scheduler median mean_dispatch=%s last_dispatch=%s completion=%s idle_slot_time=%s peak_concurrency=%d utilization=%.1f%% tick_overhead=%s; serial reference median mean_dispatch=%s last_dispatch=%s completion=%s idle_slot_time=%s peak_concurrency=%d utilization=%.1f%%",
		syntheticTasks, syntheticTasks, syntheticWorkDelay, syntheticRuns,
		schedulerMedian.meanDispatch, schedulerMedian.lastDispatch, schedulerMedian.completion,
		schedulerMedian.idleSlotTime, schedulerMedian.peakConcurrency, schedulerMedian.utilization, schedulerMedian.schedulingOverhead,
		serialMedian.meanDispatch, serialMedian.lastDispatch, serialMedian.completion,
		serialMedian.idleSlotTime, serialMedian.peakConcurrency, serialMedian.utilization)
}

// BenchmarkSyntheticFourReadyTaskTick times only the scheduler's first READY
// frontier pass. Setup and async dispatch completion are outside the timer.
// The in-memory store is intentional: this is scheduler overhead, not SQLite,
// provider launch, worker runtime, report handling or validation latency.
func BenchmarkSyntheticFourReadyTaskTick(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store := newMemoryStore(graph(
			task("synthetic-a", domain.TaskReady), task("synthetic-b", domain.TaskReady),
			task("synthetic-c", domain.TaskReady), task("synthetic-d", domain.TaskReady),
		))
		dispatcher := newSyntheticDispatcher()
		sequence := 0
		svc, err := New(store, dispatcher, agents(syntheticTasks), staticValidator{decision: ReviewAccept}, Config{
			MaxConcurrent: syntheticTasks,
			NewAttemptID:  func() string { sequence++; return fmt.Sprintf("synthetic-attempt-%d", sequence) },
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		err = svc.Tick(context.Background())
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if got := len(store.claimIDs()); got != syntheticTasks {
			b.Fatalf("Tick claimed %d tasks, want %d", got, syntheticTasks)
		}
		svc.work.Wait()
		b.StartTimer()
	}
}
