# Go Worker Engine Makefile

.PHONY: help build test test-verbose test-coverage clean run lint format deps

# Default target
help:
	@echo "Available commands:"
	@echo "  build         - Build the application"
	@echo "  test          - Run tests"
	@echo "  test-verbose  - Run tests with verbose output"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  run           - Run the application"
	@echo "  clean         - Clean build artifacts"
	@echo "  lint          - Run golangci-lint"
	@echo "  format        - Format code with gofmt"
	@echo "  deps          - Download dependencies"

# Build the application
build:
	@echo "Building worker engine..."
	@go build -o bin/workerengine cmd/main.go

# Run all tests
test:
	@echo "Running tests..."
	@go test ./...

# Run tests with verbose output
test-verbose:
	@echo "Running tests with verbose output..."
	@go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run the application
run: build
	@echo "Starting worker engine..."
	@./bin/workerengine

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f coverage.out coverage.html

# Format code
format:
	@echo "Formatting code..."
	@go fmt ./...

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

# Lint code (requires golangci-lint to be installed)
lint:
	@echo "Running linter..."
	@golangci-lint run

# Install golangci-lint
install-lint:
	@echo "Installing golangci-lint..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
