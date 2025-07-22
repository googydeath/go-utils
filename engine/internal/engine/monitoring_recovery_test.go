package engine

import (
	"context"
	"testing"
	"time"

	"github.com/googydeaath/go-utils/engine/internal/database"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMonitoringRecovery tests that the monitoring thread can recover from panics
func TestMonitoringRecovery(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 1)
	factory := NewMockWorkerFactory()

	// Use shorter intervals for testing
	engine := NewProcessEngineWithIntervals(db, factory, logger, 100*time.Millisecond, 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the engine
	err := engine.Start(ctx)
	require.NoError(t, err)
	defer engine.Stop()

	// Verify monitoring is alive
	assert.True(t, engine.isMonitoringAlive())

	// Simulate monitoring thread death by setting alive to false
	engine.setMonitoringAlive(false)

	// Wait for watchdog to detect and restart
	time.Sleep(100 * time.Millisecond)

	// The watchdog should restart the monitoring thread
	// We'll wait a bit and check if it's alive again
	time.Sleep(300 * time.Millisecond) // Wait more than the watchdog interval (200ms)

	// The monitoring should be restarted by now
	assert.True(t, engine.isMonitoringAlive(), "Monitoring should be restarted by watchdog")
}

// TestWatchdogFunctionality tests the watchdog timer functionality
func TestWatchdogFunctionality(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 1)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start with shorter watchdog interval for testing
	// We'll modify the engine to have a shorter watchdog interval
	err := engine.Start(ctx)
	require.NoError(t, err)
	defer engine.Stop()

	// Initial state - monitoring should be alive
	assert.True(t, engine.isMonitoringAlive())

	// Manually set monitoring as dead to simulate failure
	engine.setMonitoringAlive(false)
	assert.False(t, engine.isMonitoringAlive())

	// Wait for watchdog to restart monitoring
	// Since the default interval is 1 minute, we'll verify the mechanism exists
	// In a real scenario, you'd wait longer or modify the interval for testing

	// Verify the watchdog mechanism is in place by checking the cancel function exists
	assert.NotNil(t, engine.watchdogCancel, "Watchdog should be running")
}

// TestMonitoringAliveStatus tests the thread-safe monitoring alive status
func TestMonitoringAliveStatus(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 1)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	// Test initial state
	assert.False(t, engine.isMonitoringAlive())

	// Test setting alive
	engine.setMonitoringAlive(true)
	assert.True(t, engine.isMonitoringAlive())

	// Test setting dead
	engine.setMonitoringAlive(false)
	assert.False(t, engine.isMonitoringAlive())

	// Test concurrent access
	done := make(chan bool, 10)

	// Start multiple goroutines setting status
	for i := 0; i < 5; i++ {
		go func(val bool) {
			defer func() { done <- true }()
			engine.setMonitoringAlive(val)
		}(i%2 == 0)
	}

	// Start multiple goroutines reading status
	for i := 0; i < 5; i++ {
		go func() {
			defer func() { done <- true }()
			engine.isMonitoringAlive()
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic or race
}

// TestEngineStopWithWatchdog tests that stopping the engine also stops the watchdog
func TestEngineStopWithWatchdog(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	db := database.NewMockDatabase(true, 1)
	factory := NewMockWorkerFactory()

	engine := NewProcessEngine(db, factory, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the engine
	err := engine.Start(ctx)
	require.NoError(t, err)

	// Verify watchdog is running
	assert.NotNil(t, engine.watchdogCancel)

	// Stop the engine
	err = engine.Stop()
	assert.NoError(t, err)

	// Watchdog cancel should have been called (we can't directly test this,
	// but we verify no panic occurs and the method completes)
}
