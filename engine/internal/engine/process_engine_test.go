package engine

import (
	"context"
	"testing"
	"time"

	"github.com/googydeaath/go-utils/engine/internal/database"
	"github.com/googydeaath/go-utils/engine/internal/process"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockWorker for testing
type MockWorker struct {
	id      string
	runFunc func(ctx context.Context) error
}

func (m *MockWorker) ID() string {
	return m.id
}

func (m *MockWorker) Run(ctx context.Context) error {
	if m.runFunc != nil {
		return m.runFunc(ctx)
	}

	// Default behavior: run until context is cancelled
	<-ctx.Done()
	return ctx.Err()
}

// MockWorkerFactory for testing
type MockWorkerFactory struct {
	workers map[string]*MockWorker
}

func NewMockWorkerFactory() *MockWorkerFactory {
	return &MockWorkerFactory{
		workers: make(map[string]*MockWorker),
	}
}

func (f *MockWorkerFactory) CreateWorker(id string) process.Worker {
	worker := &MockWorker{id: id}
	f.workers[id] = worker
	return worker
}

func (f *MockWorkerFactory) SetWorkerRunFunc(id string, runFunc func(ctx context.Context) error) {
	if worker, exists := f.workers[id]; exists {
		worker.runFunc = runFunc
	}
}

func TestProcessEngine_StartStop(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce noise in tests

	db := database.NewMockDatabase(true, 2)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the engine
	err := engine.Start(ctx)
	require.NoError(t, err)

	// Wait a bit for processes to start
	time.Sleep(100 * time.Millisecond)

	// Check that processes were started
	stats := engine.GetStats()
	assert.True(t, stats.IsEnabled)
	assert.Equal(t, 2, stats.TargetCount)
	assert.Equal(t, 2, stats.TotalProcesses)

	// Stop the engine
	err = engine.Stop()
	assert.NoError(t, err)
}

func TestProcessEngine_ScalingUp(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 1)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start with 1 process
	err := engine.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats := engine.GetStats()
	assert.Equal(t, 1, stats.TotalProcesses)

	// Scale up to 3 processes
	db.SetProcessCount(3)

	// Trigger check
	err = engine.checkAndUpdate(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats = engine.GetStats()
	assert.Equal(t, 3, stats.TotalProcesses)
	assert.Equal(t, 3, stats.TargetCount)

	engine.Stop()
}

func TestProcessEngine_ScalingDown(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 3)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start with 3 processes
	err := engine.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats := engine.GetStats()
	assert.Equal(t, 3, stats.TotalProcesses)

	// Scale down to 1 process
	db.SetProcessCount(1)

	// Trigger check
	err = engine.checkAndUpdate(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats = engine.GetStats()
	assert.Equal(t, 1, stats.TargetCount)
	// Note: actual count might still be higher due to graceful shutdown delay

	engine.Stop()
}

func TestProcessEngine_DisableEngine(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 2)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start with engine enabled
	err := engine.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats := engine.GetStats()
	assert.True(t, stats.IsEnabled)
	assert.Equal(t, 2, stats.TotalProcesses)

	// Disable engine
	db.SetEnabled(false)

	// Trigger check
	err = engine.checkAndUpdate(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats = engine.GetStats()
	assert.False(t, stats.IsEnabled)

	engine.Stop()
}

func TestProcessEngine_FailedProcessRestart(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 1)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start engine
	err := engine.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Get the first worker and make it fail
	processes := engine.GetProcessList()
	assert.Len(t, processes, 1)

	var firstProcessID string
	for id := range processes {
		firstProcessID = id
		break
	}

	// Simulate process failure by updating status
	engine.registry.UpdateStatus(firstProcessID, StatusFailed)

	// Trigger restart check
	engine.restartFailedProcesses(ctx)

	time.Sleep(100 * time.Millisecond)

	// Should have restarted the failed process
	stats := engine.GetStats()
	assert.Equal(t, 1, stats.TotalProcesses)

	engine.Stop()
}

func TestProcessEngine_Stats(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 2)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	stats := engine.GetStats()

	assert.True(t, stats.IsEnabled)
	assert.Equal(t, 2, stats.TargetCount)
	assert.Equal(t, 2, stats.TotalProcesses)
	assert.False(t, stats.StartTime.IsZero())
	assert.False(t, stats.LastCheck.IsZero())

	engine.Stop()
}

func TestProcessEngine_IndependentContexts(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 2)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Get process contexts
	processes := engine.GetProcessList()
	assert.Len(t, processes, 2)

	var processContexts []context.Context
	var firstCancel context.CancelFunc
	for _, proc := range processes {
		processContexts = append(processContexts, proc.Context)
		if firstCancel == nil {
			firstCancel = proc.Cancel
		}
	}

	if firstCancel != nil {
		firstCancel()
	}

	time.Sleep(50 * time.Millisecond)

	// Check that only one context was cancelled
	assert.True(t, processContexts[0].Err() != nil, "First process context should be cancelled")
	assert.True(t, processContexts[1].Err() == nil, "Second process context should still be active")
	assert.True(t, ctx.Err() == nil, "Main context should still be active")

	engine.Stop()
}
