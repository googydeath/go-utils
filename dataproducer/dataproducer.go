package dataproducer

// DataProducer Provides a robust, production-ready buffered data producer with refill signaling.
// The producer manages background data fetching with comprehensive error handling,
// observability, and advanced configuration options for production environments.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Logger defines an interface for logging messages.
// This allows using custom logging implementations (e.g., structured logging).
type Logger interface {
	Printf(format string, v ...any)
	Println(v ...any)
}

// defaultLogger wraps the standard log package.
type defaultLogger struct {
	logger *log.Logger
}

func (l *defaultLogger) Printf(format string, v ...any) {
	l.logger.Printf(format, v...)
}

func (l *defaultLogger) Println(v ...any) {
	l.logger.Println(v...)
}

// FetchDataFunc defines a data retrieval contract with request parameter support.
// Type Parameters:
//   - T: Type of data items being produced
//   - R: Type of request parameter used for fetching
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - req: Request parameter (e.g., page number, cursor)
//
// Returns:
//   - []T: Slice of fetched items
//   - error: Any error that occurred during fetching
type FetchDataFunc[T any, R any] func(ctx context.Context, req R) ([]T, error)

// RequestEvolver defines an interface for evolving request parameters based on results.
// Type Parameters:
//   - R: Type of request parameter
type RequestEvolver[R any] interface {
	// Evolve returns the next request parameter based on current parameter and fetched data
	Evolve(current R, data []any) R
}

// ShutdownHook defines a function to be executed during producer shutdown
type ShutdownHook func(ctx context.Context) error

// DataProducerConfig contains initialization parameters for the producer.
// Fields:
//   - BufferSize:      Maximum items to buffer in memory
//   - FetchData:       Data retrieval implementation
//   - Request:         Initial request parameter value
//   - HalfThreshold:   Buffer level that triggers refill (typically BufferSize/2)
//   - MaxRetries:      Maximum number of fetch retry attempts before failing (0 for infinite)
//   - BaseBackoff:     Initial backoff duration for retries
//   - MaxBackoff:      Maximum backoff duration for retries
//   - FetchTimeout:    Maximum duration for a fetch operation
//   - Evolver:         Strategy to evolve request parameters (required if request needs changing)
//   - ShutdownTimeout: Maximum time to allow for graceful shutdown
//   - ShutdownHook:    Function to run during shutdown
//   - Logger:          Optional logger implementation (defaults to standard log)
//   - ErrorBufferSize: Size of the buffered error channel (defaults to 10)
type DataProducerConfig[T any, R any] struct {
	BufferSize      int
	FetchData       FetchDataFunc[T, R]
	Request         R
	HalfThreshold   int
	MaxRetries      int
	BaseBackoff     time.Duration
	MaxBackoff      time.Duration
	FetchTimeout    time.Duration
	Evolver         RequestEvolver[R] // Required if request needs to change between fetches
	ShutdownTimeout time.Duration
	ShutdownHook    ShutdownHook
	Logger          Logger // Use custom logger
	ErrorBufferSize int    // Size of the error channel buffer
}

// DataProducer manages background data fetching and buffering with production-ready features.
// It handles:
// - Initial data population
// - Refill signaling
// - Context cancellation
// - Concurrent access safety
// - Error propagation
// - Retry with exponential backoff
// - Observability metrics
type DataProducer[T any, R any] struct {
	cfg          DataProducerConfig[T, R]
	dataCh       chan T
	refillSignal chan struct{}
	errorCh      chan error // Note: Fixed-size buffer. Errors can be dropped if produced faster than consumed.
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	logger       Logger // Use the Logger interface

	// request state
	requestMu sync.Mutex
	request   R

	// retry state
	retryCount int

	// draining state for graceful shutdown
	drainingMu sync.RWMutex
	draining   bool

	// metrics
	metrics struct {
		fetchCount        int64
		fetchErrorCount   int64
		itemsProduced     int64
		refillCount       int64
		retryCount        int64
		lastFetchTime     int64 // Unix nano
		totalProcessingMs int64 // Accumulated fetch processing time in ms
	}
}

// FetchError represents a recoverable error during data fetching
type FetchError struct {
	Underlying error
	Request    any
	RetryCount int
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("fetch error on request %v (attempt %d): %v", e.Request, e.RetryCount, e.Underlying)
}

// TerminalError represents a non-recoverable error condition
type TerminalError struct {
	Underlying error
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("terminal error: %v", e.Underlying)
}

// NewDataProducer creates a ready-to-use producer instance.
// Parameters:
//   - cfg: Configuration parameters
//
// Returns:
//   - *DataProducer: Initialized producer instance
//   - error: Configuration validation error
func NewDataProducer[T any, R any](cfg DataProducerConfig[T, R]) (*DataProducer[T, R], error) {
	// Validate configuration
	if cfg.FetchData == nil {
		return nil, errors.New("FetchData function is required")
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100 // Default buffer size
	}
	if cfg.HalfThreshold <= 0 {
		cfg.HalfThreshold = cfg.BufferSize / 2
	}
	if cfg.HalfThreshold > cfg.BufferSize {
		return nil, fmt.Errorf("HalfThreshold (%d) cannot be greater than BufferSize (%d)", cfg.HalfThreshold, cfg.BufferSize)
	}
	// Allow MaxRetries = 0 for infinite retries
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 10 // Default max retries
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.BaseBackoff > cfg.MaxBackoff {
		return nil, fmt.Errorf("BaseBackoff (%v) cannot be greater than MaxBackoff (%v)", cfg.BaseBackoff, cfg.MaxBackoff)
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = 30 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.ErrorBufferSize <= 0 {
		cfg.ErrorBufferSize = 10 // Default error buffer size
	}

	// Set default logger if none provided
	logger := cfg.Logger
	if logger == nil {
		logger = &defaultLogger{
			logger: log.New(os.Stderr, "[DataProducer] ", log.LstdFlags|log.Lmicroseconds),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &DataProducer[T, R]{
		cfg:          cfg,
		dataCh:       make(chan T, cfg.BufferSize),
		refillSignal: make(chan struct{}, 1),
		errorCh:      make(chan error, cfg.ErrorBufferSize), // Use configured buffer size
		ctx:          ctx,
		cancel:       cancel,
		request:      cfg.Request,
		logger:       logger,
	}, nil
}

// Start begins background data fetching and buffering.
// Safe for concurrent use. Must be called before any consumption.
func (p *DataProducer[T, R]) Start() {
	p.wg.Add(1)
	go p.runProducer()
}

// Stop gracefully shuts down the producer:
// 1. Sets draining mode
// 2. Waits for buffer to empty or timeout
// 3. Executes shutdown hook
// 4. Cancels context and waits for in-flight operations
// 5. Closes channels
// Safe for concurrent use.
func (p *DataProducer[T, R]) Stop() {
	// Signal draining mode first
	p.setDraining(true)
	p.logger.Println("Entering draining mode for graceful shutdown")

	// Give time for draining
	shutdownTimeout := p.cfg.ShutdownTimeout
	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()

	// Watch for all data being consumed
	emptySignal := make(chan struct{})
	go func() {
		// Check periodically if channel is empty AND closed (by runProducer exiting)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if len(p.dataCh) == 0 {
					// Check if the channel is potentially closed by reading with select
					select {
					case _, ok := <-p.dataCh:
						if !ok { // Channel is closed and empty
							close(emptySignal)
							return
						}
						// Item read unexpectedly, put it back (best effort)
						// This scenario is unlikely if draining is set correctly
						p.logger.Println("Warning: Item read during draining check, attempting re-buffer")
						select {
						case p.dataCh <- *new(T): // Send zero value back
						default: // Buffer full, item lost
							p.logger.Println("Warning: Could not re-buffer item during drain check")
						}

					default: // Channel is not closed, but currently empty
						close(emptySignal)
						return
					}
				}
			case <-p.ctx.Done(): // Ensure this goroutine exits if main context is cancelled
				return
			}
		}
	}()

	// Wait for either timeout or completion
	select {
	case <-timer.C:
		p.logger.Println("Shutdown timeout exceeded, forcing stop")
	case <-emptySignal:
		p.logger.Println("Data buffer drained or producer stopped, proceeding with shutdown")
	}

	// Execute shutdown hook if provided
	if p.cfg.ShutdownHook != nil {
		// Use a separate context for the hook with its own timeout
		hookCtx, hookCancel := context.WithTimeout(context.Background(), 5*time.Second) // Hardcoded 5s timeout for hook
		defer hookCancel()

		p.logger.Println("Executing shutdown hook")
		if err := p.cfg.ShutdownHook(hookCtx); err != nil {
			p.logger.Printf("Shutdown hook error: %v", err)
		}
	}

	// Cancel context and wait for completion
	p.cancel()
	p.wg.Wait()

	// Close channels (dataCh is closed by runProducer)
	close(p.refillSignal)
	close(p.errorCh)
	p.logger.Println("Producer shutdown complete")
}

// Data provides read-only access to the buffered data channel.
// The channel closes when:
// - All data is consumed and the source is exhausted
// - Stop() is called
// - A terminal error occurs
func (p *DataProducer[T, R]) Data() <-chan T {
	return p.dataCh
}

// RefillSignal provides write access to the refill trigger channel.
// Consumers should send an empty struct when the buffer reaches HalfThreshold.
func (p *DataProducer[T, R]) RefillSignal() chan<- struct{} {
	return p.refillSignal
}

// Errors provides read-only access to the error channel.
// Consumers should monitor this channel for recoverable and terminal errors.
func (p *DataProducer[T, R]) Errors() <-chan error {
	return p.errorCh
}

// Metrics returns current operational metrics.
// Useful for monitoring and debugging.
func (p *DataProducer[T, R]) Metrics() map[string]interface{} {
	fetchCount := atomic.LoadInt64(&p.metrics.fetchCount)
	totalProcessingMs := atomic.LoadInt64(&p.metrics.totalProcessingMs)
	avgProcessingMs := int64(0)
	if fetchCount > 0 {
		avgProcessingMs = totalProcessingMs / fetchCount
	}

	bufferCurrent := len(p.dataCh)
	bufferCapacity := cap(p.dataCh)
	bufferFillRatio := 0.0
	if bufferCapacity > 0 {
		bufferFillRatio = float64(bufferCurrent) / float64(bufferCapacity)
	}

	return map[string]interface{}{
		"fetch_count":         fetchCount,
		"fetch_error_count":   atomic.LoadInt64(&p.metrics.fetchErrorCount),
		"items_produced":      atomic.LoadInt64(&p.metrics.itemsProduced),
		"buffer_current":      bufferCurrent,
		"buffer_capacity":     bufferCapacity,
		"buffer_fill_ratio":   bufferFillRatio,
		"refill_count":        atomic.LoadInt64(&p.metrics.refillCount),
		"retry_count":         atomic.LoadInt64(&p.metrics.retryCount),
		"avg_processing_ms":   avgProcessingMs,                            // Calculated average
		"total_processing_ms": totalProcessingMs,                          // Accumulated total
		"last_fetch_time_ns":  atomic.LoadInt64(&p.metrics.lastFetchTime), // Unix nano
		"is_draining":         p.isDraining(),
		"current_retry":       p.retryCount, // Note: This is the retry count for the *current* attempt, resets on success
		"max_retries":         p.cfg.MaxRetries,
		"error_channel_size":  cap(p.errorCh),
		"error_channel_len":   len(p.errorCh),
	}
}

// getRequest safely retrieves the current request state.
func (p *DataProducer[T, R]) getRequest() R {
	p.requestMu.Lock()
	defer p.requestMu.Unlock()
	return p.request
}

// setRequest safely updates the request state.
func (p *DataProducer[T, R]) setRequest(req R) {
	p.requestMu.Lock()
	defer p.requestMu.Unlock()
	p.request = req
}

// setDraining safely updates the draining state.
func (p *DataProducer[T, R]) setDraining(draining bool) {
	p.drainingMu.Lock()
	defer p.drainingMu.Unlock()
	p.draining = draining
}

// isDraining safely checks the draining state.
func (p *DataProducer[T, R]) isDraining() bool {
	p.drainingMu.RLock()
	defer p.drainingMu.RUnlock()
	return p.draining
}

// sendError attempts to send an error on the error channel without blocking.
// Logs if the error channel is full and the error is dropped.
func (p *DataProducer[T, R]) sendError(err error) {
	select {
	case p.errorCh <- err:
		// Error sent successfully
	default:
		// Error channel full, log the dropped error
		p.logger.Printf("Error channel full (capacity %d), dropping error: %v", cap(p.errorCh), err)
	}
}

// runProducer manages the core producer lifecycle:
// 1. Initial data fetch
// 2. Refill loop
// 3. Error handling with backoff
// 4. Context cancellation handling
// 5. Panic recovery
func (p *DataProducer[T, R]) runProducer() {
	defer func() {
		// Recover from panics in the main loop or fetchAndBuffer
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			err := fmt.Errorf("panic in runProducer: %v\n%s", r, stack)
			p.logger.Printf("%v", err) // Log the full error with stack

			// Send terminal error
			p.sendError(&TerminalError{
				Underlying: err,
			})
		}

		// Clean up resources
		close(p.dataCh) // Signal consumers that no more data will be produced
		p.wg.Done()
		p.logger.Println("Producer worker terminated")
	}()

	// Initial population
	p.logger.Println("Starting initial data population")
	if !p.fetchAndBuffer() {
		p.logger.Println("Initial fetch failed or was cancelled.")
		// Depending on requirements, you might want to send a terminal error here
		// if the initial fetch is absolutely critical and non-recoverable.
		// For now, let's assume the producer might recover or stop gracefully.
		return // Exit if initial fetch fails terminally or context cancels
	}

	// Refill loop
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Println("Context cancelled, exiting refill loop")
			return
		case _, ok := <-p.refillSignal:
			if !ok {
				p.logger.Println("Refill signal channel closed, exiting")
				return // Exit if signal channel is closed (likely during Stop)
			}

			// Check draining state before fetching more
			if p.isDraining() {
				p.logger.Println("In draining mode, refill signal ignored")
				continue // Don't fetch if draining
			}

			atomic.AddInt64(&p.metrics.refillCount, 1)
			p.logger.Println("Refill signal received, fetching more data")
			if !p.fetchAndBuffer() {
				p.logger.Println("Fetch loop terminated (e.g., max retries, context cancelled, end of data)")
				// If fetchAndBuffer returns false due to reaching end-of-data,
				// the loop should exit naturally. If due to error/cancellation, also exit.
				return
			}
		}
	}
}

// fetchAndBuffer handles the actual data fetching and buffering logic.
// Implements:
// - Request parameter management
// - Error backoff
// - Context cancellation checks
// - Metrics collection
// Returns false if fetching should stop (max retries, context cancelled, fatal error, end of data), true otherwise.
func (p *DataProducer[T, R]) fetchAndBuffer() bool {
	// Note: Panic recovery is handled by the caller (runProducer)

	for {
		// Check for context cancellation before attempting a fetch
		select {
		case <-p.ctx.Done():
			p.logger.Println("Context cancelled before fetch attempt")
			return false // Stop fetching
		default:
			// Continue
		}

		// Check draining state - prevent new fetches if draining
		if p.isDraining() {
			p.logger.Println("In draining mode, stopping fetch cycle")
			return false // Stop fetching
		}

		// Get current request
		req := p.getRequest()
		p.logger.Printf("Fetching data with request: %v (retry attempt: %d)", req, p.retryCount)

		// Track metrics
		atomic.AddInt64(&p.metrics.fetchCount, 1)
		startTime := time.Now()
		atomic.StoreInt64(&p.metrics.lastFetchTime, startTime.UnixNano())

		// Create timeout context for this specific fetch operation
		fetchCtx, cancel := context.WithTimeout(p.ctx, p.cfg.FetchTimeout)

		// Perform the fetch
		data, err := p.cfg.FetchData(fetchCtx, req)
		cancel() // Always cancel the fetchCtx defer

		// Calculate processing time
		processingTimeMs := time.Since(startTime).Milliseconds()
		atomic.AddInt64(&p.metrics.totalProcessingMs, processingTimeMs) // Accumulate total time

		// Handle errors
		if err != nil {
			// Check if the error is due to the fetch context deadline exceeding
			if errors.Is(err, context.DeadlineExceeded) && fetchCtx.Err() == context.DeadlineExceeded {
				p.logger.Printf("Fetch timed out after %v for request %v", p.cfg.FetchTimeout, req)
			} else if errors.Is(err, context.Canceled) && p.ctx.Err() == context.Canceled {
				// If the main context was cancelled, don't retry, just exit.
				p.logger.Println("Fetch cancelled by main context")
				return false // Stop fetching
			} else {
				p.logger.Printf("Fetch error: %v", err)
			}

			atomic.AddInt64(&p.metrics.fetchErrorCount, 1)

			// Create and send fetch error
			fetchErr := &FetchError{
				Underlying: err,
				Request:    req,
				RetryCount: p.retryCount,
			}
			p.sendError(fetchErr)

			// Apply backoff and check if retries are exhausted or context cancelled
			if !p.applyBackoff() {
				return false // Stop fetching (max retries or context cancelled during backoff)
			}
			continue // Retry the fetch
		}

		// --- Success Case ---

		// Reset retry counter on success
		p.retryCount = 0

		// Handle empty result set (normal termination condition)
		if len(data) == 0 {
			p.logger.Println("Received empty data set, assuming end of data source.")
			return false // Stop fetching, data source exhausted
		}

		// Buffer the data
		p.logger.Printf("Fetched %d items, buffering...", len(data))
		if !p.bufferData(data) {
			p.logger.Println("Context cancelled during buffer operation, stopping.")
			return false // Stop fetching (context cancelled)
		}

		// Update metrics
		atomic.AddInt64(&p.metrics.itemsProduced, int64(len(data)))

		// Update request parameter for the *next* potential fetch
		p.updateRequest(req, data)

		return true // Fetch and buffer succeeded, ready for next refill signal
	}
}

// applyBackoff implements exponential backoff with jitter for retries.
// Returns false if max retries exceeded or context cancelled, true otherwise.
func (p *DataProducer[T, R]) applyBackoff() bool {
	p.retryCount++
	atomic.AddInt64(&p.metrics.retryCount, 1)

	// Check if max retries exceeded (skip if MaxRetries is 0)
	if p.cfg.MaxRetries > 0 && p.retryCount > p.cfg.MaxRetries {
		p.logger.Printf("Max retries (%d) exceeded, terminating fetch attempts", p.cfg.MaxRetries)
		p.sendError(&TerminalError{
			Underlying: fmt.Errorf("max retries (%d) exceeded after fetch error", p.cfg.MaxRetries),
		})
		return false // Max retries exceeded
	}

	// Calculate exponential backoff duration
	backoff := p.cfg.BaseBackoff * time.Duration(math.Pow(2, float64(p.retryCount-1)))

	// Cap backoff at MaxBackoff
	if backoff > p.cfg.MaxBackoff {
		backoff = p.cfg.MaxBackoff
	}

	// Add jitter (randomize ±20% of the backoff duration)
	// Formula: backoff * (1 + (rand() * 0.4 - 0.2))
	jitter := time.Duration(float64(backoff) * (0.8 + rand.Float64()*0.4))

	retryMsg := fmt.Sprintf("retry %d", p.retryCount)
	if p.cfg.MaxRetries > 0 {
		retryMsg = fmt.Sprintf("retry %d/%d", p.retryCount, p.cfg.MaxRetries)
	}
	p.logger.Printf("Applying backoff for %v (%s)", jitter, retryMsg)

	// Wait for backoff duration or context cancellation
	select {
	case <-time.After(jitter):
		return true // Backoff complete, ready for next attempt
	case <-p.ctx.Done():
		p.logger.Println("Context cancelled during backoff")
		return false // Context cancelled
	}
}

// bufferData writes fetched items to the data channel with cancellation support.
// Returns false if context was cancelled during the buffering operation.
func (p *DataProducer[T, R]) bufferData(data []T) bool {
	for i, item := range data {
		select {
		case <-p.ctx.Done():
			p.logger.Printf("Context cancelled during buffer operation after %d items", i)
			return false // Context cancelled
		case p.dataCh <- item:
			// Item successfully buffered
		}
	}
	return true // All items buffered successfully
}

// updateRequest evolves the request parameter based on fetched data using the configured Evolver.
// If no Evolver is configured, the request parameter remains unchanged.
func (p *DataProducer[T, R]) updateRequest(currentReq R, data []T) {
	if p.cfg.Evolver == nil {
		p.logger.Println("No RequestEvolver configured, request parameter remains unchanged.")
		return
	}

	// Convert []T to []any for the Evolver interface
	// This is necessary because Go generics don't directly support variance here.
	anyData := make([]any, len(data))
	for i, d := range data {
		anyData[i] = d
	}

	newReq := p.cfg.Evolver.Evolve(currentReq, anyData)
	if any(newReq) != any(currentReq) { // Check if the request actually changed to avoid unnecessary logging/locking
		p.logger.Printf("Request evolved: %v -> %v", currentReq, newReq)
		p.setRequest(newReq)
	} else {
		p.logger.Println("Request evolution resulted in the same request parameter.")
	}
}

// --- Request Evolver Implementations ---

// PagedEvolver is a simple RequestEvolver that increments integer page numbers.
// Assumes the request type R is `int`.
type PagedEvolver struct{}

// Evolve implements RequestEvolver. It expects `current` to be an `int`
// and returns the incremented value. If `current` is not an `int`, it returns `current` unchanged.
func (e *PagedEvolver) Evolve(current int, _ []any) int {
	return current + 1
}

// CursorEvolver evolves cursor-based pagination using the last item's ID or a specific field.
// Assumes the request type R is `string` (or compatible with the cursor type).
type CursorEvolver struct {
	// ExtractCursor retrieves the cursor value (e.g., ID) from a single data item.
	// The item is passed as `any`, so the function needs to perform type assertion.
	// It should return an empty string or the original cursor if no new cursor can be extracted
	// (e.g., if the item's type is unexpected or the cursor field is missing/empty).
	// The function itself should handle potential errors during extraction.
	ExtractCursor func(item any) string
}

// Evolve implements RequestEvolver for cursor-based pagination.
// It uses the ExtractCursor function on the last item in the data slice.
// If data is empty, ExtractCursor is nil, or ExtractCursor returns an empty string,
// it returns the `current` cursor unchanged.
func (e *CursorEvolver) Evolve(current any, data []any) any {
	if len(data) == 0 || e.ExtractCursor == nil {
		return current // Cannot evolve without data or extraction logic
	}

	// Extract cursor from the last item in the fetched batch
	lastItem := data[len(data)-1]
	newCursor := e.ExtractCursor(lastItem)

	// Only update if we got a meaningful (non-empty) new cursor
	if newCursor != "" {
		// It's assumed the request type R is compatible with the string cursor.
		// A type assertion could be added here for extra safety if R is known more specifically,
		// but the generic nature makes it tricky.
		// Example safety check (if R was known to be string):
		// if _, ok := current.(string); ok { return newCursor }
		return newCursor
	}

	// Return the old cursor if extraction failed or yielded empty string
	return current
}
