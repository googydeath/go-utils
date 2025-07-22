package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/googydeaath/go-utils/engine/internal/database"
	"github.com/googydeaath/go-utils/engine/internal/process"
	"github.com/sirupsen/logrus"
)

// ProcessInfo holds information about a running process
type ProcessInfo struct {
	ID      string
	Worker  process.Worker
	Context context.Context
	Cancel  context.CancelFunc
	Started time.Time
	Status  ProcessStatus
}

// ProcessStatus represents the current status of a process
type ProcessStatus string

const (
	StatusStarting ProcessStatus = "starting"
	StatusRunning  ProcessStatus = "running"
	StatusStopping ProcessStatus = "stopping"
	StatusStopped  ProcessStatus = "stopped"
	StatusFailed   ProcessStatus = "failed"
)

// EngineStats holds statistics about the engine
type EngineStats struct {
	TotalProcesses   int
	RunningProcesses int
	FailedProcesses  int
	StartTime        time.Time
	LastCheck        time.Time
	IsEnabled        bool
	TargetCount      int
}

// ProcessEngine manages multiple worker processes
type ProcessEngine struct {
	db               database.DatabaseInterface
	factory          process.WorkerFactory
	logger           *logrus.Logger
	registry         *ProcessRegistry
	stats            EngineStats
	statsMux         sync.RWMutex
	monitorTicker    *time.Ticker
	monitorCancel    context.CancelFunc
	enabled          bool
	targetCount      int
	monitoringAlive  bool
	aliveMux         sync.RWMutex
	watchdogCancel   context.CancelFunc
	monitorInterval  time.Duration
	watchdogInterval time.Duration
}

// NewProcessEngine creates a new process engine
func NewProcessEngine(db database.DatabaseInterface, factory process.WorkerFactory, logger *logrus.Logger) *ProcessEngine {
	return &ProcessEngine{
		db:               db,
		factory:          factory,
		logger:           logger,
		registry:         NewProcessRegistry(),
		monitorInterval:  5 * time.Minute,
		watchdogInterval: 1 * time.Minute,
		stats: EngineStats{
			StartTime: time.Now(),
		},
	}
}

// NewProcessEngineWithIntervals creates a new process engine with custom intervals (for testing)
func NewProcessEngineWithIntervals(db database.DatabaseInterface, factory process.WorkerFactory, logger *logrus.Logger, monitorInterval, watchdogInterval time.Duration) *ProcessEngine {
	return &ProcessEngine{
		db:               db,
		factory:          factory,
		logger:           logger,
		registry:         NewProcessRegistry(),
		monitorInterval:  monitorInterval,
		watchdogInterval: watchdogInterval,
		stats: EngineStats{
			StartTime: time.Now(),
		},
	}
}

// Start begins the process engine operation
func (pe *ProcessEngine) Start(ctx context.Context) error {
	pe.logger.Info("Starting Process Engine")

	// Start monitoring goroutine with recovery
	monitorCtx, cancel := context.WithCancel(ctx)
	pe.monitorCancel = cancel
	pe.monitorTicker = time.NewTicker(pe.monitorInterval)

	// Start the monitoring with recovery
	go pe.startMonitoringWithRecovery(monitorCtx)

	// Start watchdog to monitor the monitoring thread
	watchdogCtx, watchdogCancel := context.WithCancel(ctx)
	pe.watchdogCancel = watchdogCancel
	go pe.watchdogLoop(watchdogCtx)

	// Initial check and startup
	if err := pe.checkAndUpdate(ctx); err != nil {
		pe.logger.WithError(err).Error("Initial check failed")
		return fmt.Errorf("initial check failed: %w", err)
	}

	pe.logger.Info("Process Engine started successfully")
	return nil
}

// Stop gracefully shuts down the process engine
func (pe *ProcessEngine) Stop() error {
	pe.logger.Info("Stopping Process Engine")

	// Stop watchdog
	if pe.watchdogCancel != nil {
		pe.watchdogCancel()
	}

	// Stop monitoring
	if pe.monitorCancel != nil {
		pe.monitorCancel()
	}
	if pe.monitorTicker != nil {
		pe.monitorTicker.Stop()
	}

	// Stop all processes
	pe.stopAllProcesses()

	pe.logger.Info("Process Engine stopped")
	return nil
}

// startMonitoringWithRecovery starts monitoring with panic recovery
func (pe *ProcessEngine) startMonitoringWithRecovery(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			pe.logger.WithField("panic", r).Error("Monitoring loop panicked, will be restarted by watchdog")
			pe.setMonitoringAlive(false)
		}
	}()

	pe.setMonitoringAlive(true)
	pe.monitorLoop(ctx)
	pe.setMonitoringAlive(false)
}

// monitorLoop runs the monitoring logic every 5 minutes
func (pe *ProcessEngine) monitorLoop(ctx context.Context) {
	pe.logger.Info("Starting monitoring loop")

	for {
		select {
		case <-ctx.Done():
			pe.logger.Info("Monitoring loop stopping")
			return
		case <-pe.monitorTicker.C:
			pe.logger.Debug("Running scheduled monitoring check")
			if err := pe.checkAndUpdate(ctx); err != nil {
				pe.logger.WithError(err).Error("Monitoring check failed")
			}
			pe.printStats()
		}
	}
}

// watchdogLoop monitors the monitoring thread and restarts it if it dies
func (pe *ProcessEngine) watchdogLoop(ctx context.Context) {
	pe.logger.Info("Starting monitoring watchdog")
	watchdogTicker := time.NewTicker(pe.watchdogInterval)
	defer watchdogTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			pe.logger.Info("Monitoring watchdog stopping")
			return
		case <-watchdogTicker.C:
			if !pe.isMonitoringAlive() {
				pe.logger.Warn("Monitoring thread appears to be dead, restarting...")

				// Cancel old monitoring context and create new one
				if pe.monitorCancel != nil {
					pe.monitorCancel()
				}

				monitorCtx, cancel := context.WithCancel(ctx)
				pe.monitorCancel = cancel

				// Restart monitoring
				go pe.startMonitoringWithRecovery(monitorCtx)
			}
		}
	}
}

// setMonitoringAlive safely sets the monitoring alive status
func (pe *ProcessEngine) setMonitoringAlive(alive bool) {
	pe.aliveMux.Lock()
	defer pe.aliveMux.Unlock()
	pe.monitoringAlive = alive
}

// isMonitoringAlive safely checks if monitoring is alive
func (pe *ProcessEngine) isMonitoringAlive() bool {
	pe.aliveMux.RLock()
	defer pe.aliveMux.RUnlock()
	return pe.monitoringAlive
}

// checkAndUpdate performs the main monitoring logic
func (pe *ProcessEngine) checkAndUpdate(ctx context.Context) error {
	pe.updateStats()

	// Check if engine should be enabled
	enabled, err := pe.db.IsEnabled(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if enabled: %w", err)
	}

	pe.enabled = enabled

	if !enabled {
		pe.logger.Info("Engine disabled, stopping all processes")
		pe.stopAllProcesses()
		pe.updateStats()
		return nil
	}

	// Get target process count
	targetCount, err := pe.db.ProcessCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get process count: %w", err)
	}

	pe.targetCount = targetCount

	// Scale processes to match target
	if err := pe.scaleProcesses(ctx, targetCount); err != nil {
		return fmt.Errorf("failed to scale processes: %w", err)
	}

	// Check for failed processes and restart them
	pe.restartFailedProcesses(ctx)

	pe.updateStats()
	return nil
}

// scaleProcesses adjusts the number of running processes to match the target
func (pe *ProcessEngine) scaleProcesses(ctx context.Context, targetCount int) error {
	currentCount := pe.registry.Size()

	if currentCount < targetCount {
		// Scale up
		toStart := targetCount - currentCount
		pe.logger.WithFields(logrus.Fields{
			"current": currentCount,
			"target":  targetCount,
			"scaling": "up",
			"count":   toStart,
		}).Info("Scaling processes")

		for i := 0; i < toStart; i++ {
			if err := pe.startProcess(ctx); err != nil {
				pe.logger.WithError(err).Error("Failed to start process during scale up")
				return err
			}
		}
	} else if currentCount > targetCount {
		// Scale down
		toStop := currentCount - targetCount
		pe.logger.WithFields(logrus.Fields{
			"current": currentCount,
			"target":  targetCount,
			"scaling": "down",
			"count":   toStop,
		}).Info("Scaling processes")

		runningProcesses := pe.registry.GetRunningProcesses()
		for i, id := range runningProcesses {
			if i >= toStop {
				break
			}
			pe.stopProcess(id)
		}
	}

	return nil
}

// startProcess creates and starts a new worker process
func (pe *ProcessEngine) startProcess(ctx context.Context) error {
	id := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	worker := pe.factory.CreateWorker(id)

	// Create independent context for this process
	processCtx, cancel := context.WithCancel(context.Background())

	processInfo := &ProcessInfo{
		ID:      id,
		Worker:  worker,
		Context: processCtx,
		Cancel:  cancel,
		Started: time.Now(),
		Status:  StatusStarting,
	}

	pe.registry.Add(id, processInfo)

	pe.logger.WithField("process_id", id).Info("Starting new process")

	// Start the worker in a goroutine
	go func() {
		pe.registry.UpdateStatus(id, StatusRunning)

		if err := worker.Run(processCtx); err != nil {
			if processCtx.Err() != nil {
				pe.registry.UpdateStatus(id, StatusStopped)
				pe.logger.WithField("process_id", id).Info("Process stopped gracefully")
			} else {
				pe.registry.UpdateStatus(id, StatusFailed)
				pe.logger.WithFields(logrus.Fields{
					"process_id": id,
					"error":      err,
				}).Error("Process failed")
			}
		} else {
			pe.registry.UpdateStatus(id, StatusStopped)
			pe.logger.WithField("process_id", id).Info("Process completed normally")
		}
	}()

	return nil
}

// stopProcess gracefully stops a specific process
func (pe *ProcessEngine) stopProcess(id string) {
	pe.logger.WithField("process_id", id).Info("Stopping process")

	if proc, exists := pe.registry.Get(id); exists {
		pe.registry.UpdateStatus(id, StatusStopping)
		proc.Cancel()

		// Give it some time to stop gracefully, then remove from registry
		go func() {
			time.Sleep(30 * time.Second)
			pe.registry.Remove(id)
		}()
	}
}

// stopAllProcesses stops all currently running processes
func (pe *ProcessEngine) stopAllProcesses() {
	count := pe.registry.Size()
	pe.logger.WithField("count", count).Info("Stopping all processes")

	processes := pe.registry.GetAll()
	for id, proc := range processes {
		pe.registry.UpdateStatus(id, StatusStopping)
		proc.Cancel()
	}

	// Wait for graceful shutdown
	time.Sleep(5 * time.Second)

	// Clear the registry
	pe.registry.Clear()
}

// restartFailedProcesses checks for failed processes and restarts them
func (pe *ProcessEngine) restartFailedProcesses(ctx context.Context) {
	failedProcesses := pe.registry.GetFailedProcesses()

	for _, id := range failedProcesses {
		pe.logger.WithField("process_id", id).Warn("Restarting failed process")
		pe.registry.Remove(id)

		// Start a new process to replace the failed one
		if err := pe.startProcess(ctx); err != nil {
			pe.logger.WithError(err).Error("Failed to restart process")
		}
	}
}

// updateStats updates the engine statistics
func (pe *ProcessEngine) updateStats() {
	pe.statsMux.Lock()
	defer pe.statsMux.Unlock()

	total, running, failed := pe.registry.GetCounts()

	pe.stats.TotalProcesses = int(total)
	pe.stats.RunningProcesses = int(running)
	pe.stats.FailedProcesses = int(failed)
	pe.stats.LastCheck = time.Now()
	pe.stats.IsEnabled = pe.enabled
	pe.stats.TargetCount = pe.targetCount
}

// GetStats returns a copy of the current engine statistics
func (pe *ProcessEngine) GetStats() EngineStats {
	pe.statsMux.RLock()
	defer pe.statsMux.RUnlock()
	return pe.stats
}

// printStats logs the current engine statistics
func (pe *ProcessEngine) printStats() {
	stats := pe.GetStats()

	pe.logger.WithFields(logrus.Fields{
		"total_processes":   stats.TotalProcesses,
		"running_processes": stats.RunningProcesses,
		"failed_processes":  stats.FailedProcesses,
		"is_enabled":        stats.IsEnabled,
		"target_count":      stats.TargetCount,
		"uptime":            time.Since(stats.StartTime).String(),
		"last_check":        stats.LastCheck.Format(time.RFC3339),
	}).Info("Engine Statistics")
}

// GetProcessList returns information about all current processes
func (pe *ProcessEngine) GetProcessList() map[string]ProcessInfo {
	processes := pe.registry.GetAll()
	result := make(map[string]ProcessInfo)
	for id, proc := range processes {
		result[id] = *proc
	}
	return result
}
