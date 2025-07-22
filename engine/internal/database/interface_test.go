package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMockDatabase_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		expected bool
	}{
		{"enabled", true, true},
		{"disabled", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := NewMockDatabase(tt.enabled, 0)
			ctx := context.Background()

			result, err := db.IsEnabled(ctx)

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMockDatabase_ProcessCount(t *testing.T) {
	tests := []struct {
		name         string
		processCount int
		expected     int
	}{
		{"zero processes", 0, 0},
		{"one process", 1, 1},
		{"multiple processes", 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := NewMockDatabase(true, tt.processCount)
			ctx := context.Background()

			result, err := db.ProcessCount(ctx)

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMockDatabase_SetEnabled(t *testing.T) {
	db := NewMockDatabase(true, 0)
	ctx := context.Background()

	// Initially enabled
	result, err := db.IsEnabled(ctx)
	assert.NoError(t, err)
	assert.True(t, result)

	// Set to disabled
	db.SetEnabled(false)
	result, err = db.IsEnabled(ctx)
	assert.NoError(t, err)
	assert.False(t, result)

	// Set back to enabled
	db.SetEnabled(true)
	result, err = db.IsEnabled(ctx)
	assert.NoError(t, err)
	assert.True(t, result)
}

func TestMockDatabase_SetProcessCount(t *testing.T) {
	db := NewMockDatabase(true, 1)
	ctx := context.Background()

	// Initially 1
	result, err := db.ProcessCount(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, result)

	// Set to 5
	db.SetProcessCount(5)
	result, err = db.ProcessCount(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 5, result)

	// Set to 0
	db.SetProcessCount(0)
	result, err = db.ProcessCount(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, result)
}
