package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/googydeaath/go-utils/engine/internal/database"
	"github.com/googydeaath/go-utils/engine/internal/engine"
	"github.com/googydeaath/go-utils/engine/internal/process"
	"github.com/sirupsen/logrus"
)

func main() {
	// Configure logging
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02T15:04:05.000Z",
	})

	logger.Info("Starting Worker Engine")

	// Create database interface (using mock for this example)
	db := database.NewMockDatabase(true, 3)

	// Create worker factory
	workerFactory := process.NewExampleWorkerFactory(logger)

	// Create process engine
	processEngine := engine.NewProcessEngine(db, workerFactory, logger)

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signalChan
		logger.WithField("signal", sig).Info("Received shutdown signal")
		cancel()
	}()

	// Start the process engine
	if err := processEngine.Start(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to start process engine")
	}

	// Wait for context cancellation
	<-ctx.Done()

	logger.Info("Shutting down Worker Engine")

	// Stop the process engine
	if err := processEngine.Stop(); err != nil {
		logger.WithError(err).Error("Error stopping process engine")
	}

	logger.Info("Worker Engine shutdown complete")
}
