package database

import "context"

// DatabaseInterface defines the contract for database operations
type DatabaseInterface interface {
	// IsEnabled checks if the process engine should be active
	IsEnabled(ctx context.Context) (bool, error)
	
	// ProcessCount returns the number of processes that should be running
	ProcessCount(ctx context.Context) (int, error)
}

// MockDatabase implements DatabaseInterface for testing and example purposes
type MockDatabase struct {
	enabled      bool
	processCount int
}

// NewMockDatabase creates a new mock database instance
func NewMockDatabase(enabled bool, processCount int) *MockDatabase {
	return &MockDatabase{
		enabled:      enabled,
		processCount: processCount,
	}
}

// IsEnabled returns the configured enabled status
func (m *MockDatabase) IsEnabled(ctx context.Context) (bool, error) {
	return m.enabled, nil
}

// ProcessCount returns the configured process count
func (m *MockDatabase) ProcessCount(ctx context.Context) (int, error) {
	return m.processCount, nil
}

// SetEnabled updates the enabled status (for testing)
func (m *MockDatabase) SetEnabled(enabled bool) {
	m.enabled = enabled
}

// SetProcessCount updates the process count (for testing)
func (m *MockDatabase) SetProcessCount(count int) {
	m.processCount = count
}
