package engine

import (
	"sync"
	"sync/atomic"
)

// ProcessRegistry manages the process registry with thread-safe operations
type ProcessRegistry struct {
	processes    sync.Map // map[string]*ProcessInfo
	totalCount   int64    // atomic counter for total processes
	runningCount int64    // atomic counter for running processes
	failedCount  int64    // atomic counter for failed processes
}

// NewProcessRegistry creates a new process registry
func NewProcessRegistry() *ProcessRegistry {
	return &ProcessRegistry{}
}

// Add adds a process to the registry
func (pr *ProcessRegistry) Add(id string, processInfo *ProcessInfo) {
	pr.processes.Store(id, processInfo)
	atomic.AddInt64(&pr.totalCount, 1)
	
	if processInfo.Status == StatusRunning {
		atomic.AddInt64(&pr.runningCount, 1)
	} else if processInfo.Status == StatusFailed {
		atomic.AddInt64(&pr.failedCount, 1)
	}
}

// Remove removes a process from the registry
func (pr *ProcessRegistry) Remove(id string) {
	if value, exists := pr.processes.LoadAndDelete(id); exists {
		if processInfo, ok := value.(*ProcessInfo); ok {
			atomic.AddInt64(&pr.totalCount, -1)
			
			if processInfo.Status == StatusRunning {
				atomic.AddInt64(&pr.runningCount, -1)
			} else if processInfo.Status == StatusFailed {
				atomic.AddInt64(&pr.failedCount, -1)
			}
		}
	}
}

// Get retrieves a process from the registry
func (pr *ProcessRegistry) Get(id string) (*ProcessInfo, bool) {
	if value, exists := pr.processes.Load(id); exists {
		if processInfo, ok := value.(*ProcessInfo); ok {
			return processInfo, true
		}
	}
	return nil, false
}

// UpdateStatus updates the status of a process and adjusts counters
func (pr *ProcessRegistry) UpdateStatus(id string, newStatus ProcessStatus) {
	if value, exists := pr.processes.Load(id); exists {
		if processInfo, ok := value.(*ProcessInfo); ok {
			oldStatus := processInfo.Status
			
			// Update counters based on status change
			if oldStatus == StatusRunning && newStatus != StatusRunning {
				atomic.AddInt64(&pr.runningCount, -1)
			} else if oldStatus != StatusRunning && newStatus == StatusRunning {
				atomic.AddInt64(&pr.runningCount, 1)
			}
			
			if oldStatus == StatusFailed && newStatus != StatusFailed {
				atomic.AddInt64(&pr.failedCount, -1)
			} else if oldStatus != StatusFailed && newStatus == StatusFailed {
				atomic.AddInt64(&pr.failedCount, 1)
			}
			
			processInfo.Status = newStatus
		}
	}
}

// GetAll returns all processes as a map
func (pr *ProcessRegistry) GetAll() map[string]*ProcessInfo {
	result := make(map[string]*ProcessInfo)
	pr.processes.Range(func(key, value interface{}) bool {
		if id, ok := key.(string); ok {
			if processInfo, ok := value.(*ProcessInfo); ok {
				// Create a copy to avoid race conditions
				result[id] = &ProcessInfo{
					ID:      processInfo.ID,
					Worker:  processInfo.Worker,
					Context: processInfo.Context,
					Cancel:  processInfo.Cancel,
					Started: processInfo.Started,
					Status:  processInfo.Status,
				}
			}
		}
		return true
	})
	return result
}

// GetFailedProcesses returns all failed processes
func (pr *ProcessRegistry) GetFailedProcesses() []string {
	var failed []string
	pr.processes.Range(func(key, value interface{}) bool {
		if id, ok := key.(string); ok {
			if processInfo, ok := value.(*ProcessInfo); ok && processInfo.Status == StatusFailed {
				failed = append(failed, id)
			}
		}
		return true
	})
	return failed
}

// GetRunningProcesses returns all running process IDs
func (pr *ProcessRegistry) GetRunningProcesses() []string {
	var running []string
	pr.processes.Range(func(key, value interface{}) bool {
		if id, ok := key.(string); ok {
			if processInfo, ok := value.(*ProcessInfo); ok && 
				(processInfo.Status == StatusRunning || processInfo.Status == StatusStarting) {
				running = append(running, id)
			}
		}
		return true
	})
	return running
}

// GetCounts returns the current counts
func (pr *ProcessRegistry) GetCounts() (total, running, failed int64) {
	return atomic.LoadInt64(&pr.totalCount), 
		   atomic.LoadInt64(&pr.runningCount), 
		   atomic.LoadInt64(&pr.failedCount)
}

// Clear removes all processes from the registry
func (pr *ProcessRegistry) Clear() {
	pr.processes.Range(func(key, value interface{}) bool {
		pr.processes.Delete(key)
		return true
	})
	atomic.StoreInt64(&pr.totalCount, 0)
	atomic.StoreInt64(&pr.runningCount, 0)
	atomic.StoreInt64(&pr.failedCount, 0)
}

// Size returns the total number of processes
func (pr *ProcessRegistry) Size() int {
	return int(atomic.LoadInt64(&pr.totalCount))
}
