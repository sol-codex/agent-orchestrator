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
	meanDispatch time.Duration
	lastDispatch time.Duration
	completion   time.Duration
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
	completion := time.Since(readyAt)
	cancel()
	<-done
	starts := dispatcher.starts()
	if len(starts) != syntheticTasks {
		t.Fatalf("synthetic scheduler dispatched %d tasks, want %d", len(starts), syntheticTasks)
	}
	var total time.Duration
	for _, started := range starts {
		total += started.Sub(readyAt)
	}
	return syntheticTiming{meanDispatch: total / syntheticTasks, lastDispatch: starts[len(starts)-1].Sub(readyAt), completion: completion}
}

func syntheticSerialRun() syntheticTiming {
	readyAt := time.Now()
	var total time.Duration
	var last time.Duration
	for i := 0; i < syntheticTasks; i++ {
		last = time.Since(readyAt)
		total += last
		time.Sleep(syntheticWorkDelay)
	}
	return syntheticTiming{meanDispatch: total / syntheticTasks, lastDispatch: last, completion: time.Since(readyAt)}
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
		meanDispatch: median(func(sample syntheticTiming) time.Duration { return sample.meanDispatch }),
		lastDispatch: median(func(sample syntheticTiming) time.Duration { return sample.lastDispatch }),
		completion:   median(func(sample syntheticTiming) time.Duration { return sample.completion }),
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
	t.Logf("synthetic workload: tasks=%d worker_limit=%d worker_delay=%s runs=%d; scheduler median mean_dispatch=%s last_dispatch=%s completion=%s; serial reference median mean_dispatch=%s last_dispatch=%s completion=%s",
		syntheticTasks, syntheticTasks, syntheticWorkDelay, syntheticRuns,
		schedulerMedian.meanDispatch, schedulerMedian.lastDispatch, schedulerMedian.completion,
		serialMedian.meanDispatch, serialMedian.lastDispatch, serialMedian.completion)
}
