package utils

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func WithGracefulShutdown(logger *slog.Logger) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		logger.Info("Shutting down...")
		cancel()
	}()

	return ctx
}
