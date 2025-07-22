package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessRegistry_AddAndGet(t *testing.T) {
	registry := NewProcessRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	processInfo := &ProcessInfo{
		ID:      "test-1",
		Worker:  &MockWorker{id: "test-1"},
		Context: ctx,
		Cancel:  cancel,
		Started: time.Now(),
		Status:  StatusStarting,
	}
	
	// Add process
	registry.Add("test-1", processInfo)
	
	// Check if it exists
	retrieved, exists := registry.Get("test-1")
	assert.True(t, exists)
	assert.Equal(t, "test-1", retrieved.ID)
	assert.Equal(t, StatusStarting, retrieved.Status)
	
	// Check counts
	total, running, failed := registry.GetCounts()
	assert.Equal(t, int64(1), total)
	assert.Equal(t, int64(0), running) // Starting status
	assert.Equal(t, int64(0), failed)
}

func TestProcessRegistry_UpdateStatus(t *testing.T) {
	registry := NewProcessRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	processInfo := &ProcessInfo{
		ID:      "test-1",
		Worker:  &MockWorker{id: "test-1"},
		Context: ctx,
		Cancel:  cancel,
		Started: time.Now(),
		Status:  StatusStarting,
	}
	
	registry.Add("test-1", processInfo)
	
	// Update to running
	registry.UpdateStatus("test-1", StatusRunning)
	
	retrieved, exists := registry.Get("test-1")
	assert.True(t, exists)
	assert.Equal(t, StatusRunning, retrieved.Status)
	
	// Check counts
	total, running, failed := registry.GetCounts()
	assert.Equal(t, int64(1), total)
	assert.Equal(t, int64(1), running)
	assert.Equal(t, int64(0), failed)
	
	// Update to failed
	registry.UpdateStatus("test-1", StatusFailed)
	
	total, running, failed = registry.GetCounts()
	assert.Equal(t, int64(1), total)
	assert.Equal(t, int64(0), running)
	assert.Equal(t, int64(1), failed)
}

func TestProcessRegistry_Remove(t *testing.T) {
	registry := NewProcessRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	processInfo := &ProcessInfo{
		ID:      "test-1",
		Worker:  &MockWorker{id: "test-1"},
		Context: ctx,
		Cancel:  cancel,
		Started: time.Now(),
		Status:  StatusRunning,
	}
	
	registry.Add("test-1", processInfo)
	
	// Verify it exists
	_, exists := registry.Get("test-1")
	assert.True(t, exists)
	
	total, running, failed := registry.GetCounts()
	assert.Equal(t, int64(1), total)
	assert.Equal(t, int64(1), running)
	
	// Remove it
	registry.Remove("test-1")
	
	// Verify it's gone
	_, exists = registry.Get("test-1")
	assert.False(t, exists)
	
	total, running, failed = registry.GetCounts()
	assert.Equal(t, int64(0), total)
	assert.Equal(t, int64(0), running)
	assert.Equal(t, int64(0), failed)
}

func TestProcessRegistry_GetFailedProcesses(t *testing.T) {
	registry := NewProcessRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Add multiple processes
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("test-%d", i)
		processInfo := &ProcessInfo{
			ID:      id,
			Worker:  &MockWorker{id: id},
			Context: ctx,
			Cancel:  cancel,
			Started: time.Now(),
			Status:  StatusRunning,
		}
		registry.Add(id, processInfo)
	}
	
	// Mark some as failed
	registry.UpdateStatus("test-1", StatusFailed)
	registry.UpdateStatus("test-3", StatusFailed)
	
	failedProcesses := registry.GetFailedProcesses()
	assert.Len(t, failedProcesses, 2)
	assert.Contains(t, failedProcesses, "test-1")
	assert.Contains(t, failedProcesses, "test-3")
}

func TestProcessRegistry_GetRunningProcesses(t *testing.T) {
	registry := NewProcessRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Add multiple processes
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("test-%d", i)
		status := StatusRunning
		if i == 2 {
			status = StatusFailed
		}
		if i == 4 {
			status = StatusStopped
		}
		
		processInfo := &ProcessInfo{
			ID:      id,
			Worker:  &MockWorker{id: id},
			Context: ctx,
			Cancel:  cancel,
			Started: time.Now(),
			Status:  status,
		}
		registry.Add(id, processInfo)
	}
	
	runningProcesses := registry.GetRunningProcesses()
	assert.Len(t, runningProcesses, 3) // test-0, test-1, test-3
}

func TestProcessRegistry_Clear(t *testing.T) {
	registry := NewProcessRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Add multiple processes
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("test-%d", i)
		processInfo := &ProcessInfo{
			ID:      id,
			Worker:  &MockWorker{id: id},
			Context: ctx,
			Cancel:  cancel,
			Started: time.Now(),
			Status:  StatusRunning,
		}
		registry.Add(id, processInfo)
	}
	
	// Verify they exist
	assert.Equal(t, 3, registry.Size())
	
	// Clear all
	registry.Clear()
	
	// Verify they're gone
	assert.Equal(t, 0, registry.Size())
	total, running, failed := registry.GetCounts()
	assert.Equal(t, int64(0), total)
	assert.Equal(t, int64(0), running)
	assert.Equal(t, int64(0), failed)
}

func TestProcessRegistry_ConcurrentAccess(t *testing.T) {
	registry := NewProcessRegistry()
	
	// Test concurrent adds and reads
	done := make(chan bool, 10)
	
	// Start multiple goroutines adding processes
	for i := 0; i < 5; i++ {
		go func(id int) {
			defer func() { done <- true }()
			
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			
			processInfo := &ProcessInfo{
				ID:      fmt.Sprintf("concurrent-%d", id),
				Worker:  &MockWorker{id: fmt.Sprintf("concurrent-%d", id)},
				Context: ctx,
				Cancel:  cancel,
				Started: time.Now(),
				Status:  StatusRunning,
			}
			
			registry.Add(processInfo.ID, processInfo)
		}(i)
	}
	
	// Start multiple goroutines reading
	for i := 0; i < 5; i++ {
		go func() {
			defer func() { done <- true }()
			
			// Read operations
			registry.GetAll()
			registry.GetCounts()
			registry.GetRunningProcesses()
		}()
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify final state
	assert.Equal(t, 5, registry.Size())
}
