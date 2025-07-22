package process

import (
	"context"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
)

// Worker represents a single worker process
type Worker interface {
	// Run starts the worker process
	Run(ctx context.Context) error
	
	// ID returns the unique identifier for this worker
	ID() string
}

// ExampleWorker is a sample implementation of the Worker interface
type ExampleWorker struct {
	id     string
	logger *logrus.Entry
}

// NewExampleWorker creates a new example worker
func NewExampleWorker(id string, logger *logrus.Logger) *ExampleWorker {
	return &ExampleWorker{
		id:     id,
		logger: logger.WithField("worker_id", id),
	}
}

// ID returns the worker's unique identifier
func (w *ExampleWorker) ID() string {
	return w.id
}

// Run executes the worker's main loop
func (w *ExampleWorker) Run(ctx context.Context) error {
	w.logger.Info("Worker starting")
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Worker shutting down gracefully")
			return ctx.Err()
		case <-ticker.C:
			// Simulate work
			w.doWork()
		}
	}
}

func (w *ExampleWorker) doWork() {
	// Simulate some work with random duration
	workDuration := time.Duration(rand.Intn(5)+1) * time.Second
	w.logger.WithField("duration", workDuration).Debug("Starting work")
	
	time.Sleep(workDuration)
	
	w.logger.Debug("Work completed")
	
	// Simulate occasional errors (5% chance)
	if rand.Intn(100) < 5 {
		w.logger.Error("Simulated work error occurred")
	}
}

// WorkerFactory creates new workers
type WorkerFactory interface {
	CreateWorker(id string) Worker
}

// ExampleWorkerFactory creates ExampleWorker instances
type ExampleWorkerFactory struct {
	logger *logrus.Logger
}

// NewExampleWorkerFactory creates a new worker factory
func NewExampleWorkerFactory(logger *logrus.Logger) *ExampleWorkerFactory {
	return &ExampleWorkerFactory{logger: logger}
}

// CreateWorker creates a new ExampleWorker
func (f *ExampleWorkerFactory) CreateWorker(id string) Worker {
	return NewExampleWorker(id, f.logger)
}
