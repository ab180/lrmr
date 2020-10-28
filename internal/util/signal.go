package util

import (
	"context"
	"os"
	"os/signal"

	"github.com/airbloc/logger"
)

func ContextWithSignal(parent context.Context, sig ...os.Signal) (context.Context, context.CancelFunc) {
	log := logger.New("lrmr.util")

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, sig...)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case sig := <-sigChan:
			log.Verbose("{} received during execution.", sig.String())
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(sigChan)
		cancel()
	}
}
