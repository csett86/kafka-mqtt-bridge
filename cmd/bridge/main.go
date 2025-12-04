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
	configPath := flag.String("config", "", "Path to configuration file (optional; if not provided, configuration is loaded from environment variables)")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	var cfg *config.Config
	if *configPath != "" {
		cfg, err = config.LoadConfig(*configPath)
		if err != nil {
			logger.Fatal("Failed to load configuration from file", zap.String("config", *configPath), zap.Error(err))
		}
		logger.Info("Configuration loaded from file",
			zap.String("config", *configPath),
		)
	} else {
		cfg, err = config.LoadConfigFromEnv()
		if err != nil {
			logger.Fatal("Failed to load configuration from environment variables", zap.Error(err))
		}
		logger.Info("Configuration loaded from environment variables")
	}

	logger.Info("Starting Kafka-MQTT Bridge",
		zap.String("bridge_name", cfg.Bridge.Name),
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
