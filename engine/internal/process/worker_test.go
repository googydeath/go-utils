package process

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExampleWorker_ID(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	
	worker := NewExampleWorker("test-id", logger)
	
	assert.Equal(t, "test-id", worker.ID())
}

func TestExampleWorker_Run_GracefulShutdown(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	
	worker := NewExampleWorker("test-worker", logger)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	// Start the worker in a goroutine
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	
	// Let it run for a short time
	time.Sleep(50 * time.Millisecond)
	
	// Cancel the context
	cancel()
	
	// Wait for completion
	select {
	case err := <-done:
		assert.Equal(t, context.Canceled, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not shut down within timeout")
	}
}

func TestExampleWorker_Run_WithTimeout(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	
	worker := NewExampleWorker("test-worker", logger)
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	// Start the worker
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	
	// Wait for completion
	select {
	case err := <-done:
		assert.Equal(t, context.DeadlineExceeded, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not shut down within timeout")
	}
}

func TestExampleWorkerFactory_CreateWorker(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	
	factory := NewExampleWorkerFactory(logger)
	
	worker := factory.CreateWorker("test-id")
	
	require.NotNil(t, worker)
	assert.Equal(t, "test-id", worker.ID())
	
	// Verify it's the correct type
	exampleWorker, ok := worker.(*ExampleWorker)
	require.True(t, ok)
	assert.Equal(t, "test-id", exampleWorker.id)
}

func TestExampleWorkerFactory_CreateMultipleWorkers(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	
	factory := NewExampleWorkerFactory(logger)
	
	worker1 := factory.CreateWorker("worker-1")
	worker2 := factory.CreateWorker("worker-2")
	
	require.NotNil(t, worker1)
	require.NotNil(t, worker2)
	
	assert.Equal(t, "worker-1", worker1.ID())
	assert.Equal(t, "worker-2", worker2.ID())
	
	// Verify they are different instances
	assert.NotEqual(t, worker1, worker2)
}

func TestExampleWorker_DoWork(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	
	worker := NewExampleWorker("test-worker", logger)
	
	// This test just verifies that doWork doesn't panic
	// The actual work simulation is random, so we can't test specific outcomes
	assert.NotPanics(t, func() {
		worker.doWork()
	})
}
