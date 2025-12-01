package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/csett86/kafka-mqtt-bridge/internal/bridge"
	"github.com/csett86/kafka-mqtt-bridge/pkg/config"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "config/config.yaml", "Path to configuration file")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	logger.Info("Starting Kafka-MQTT Bridge",
		zap.String("version", "1.0.0"),
		zap.String("config", *configPath),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := bridge.New(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize bridge", zap.Error(err))
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		errChan <- b.Start(ctx)
	}()

	select {
	case <-sigChan:
		logger.Info("Received shutdown signal, gracefully shutting down")
		cancel()
	case err := <-errChan:
		logger.Fatal("Bridge error", zap.Error(err))
	}

	b.Stop()
	logger.Info("Bridge stopped successfully")
}
