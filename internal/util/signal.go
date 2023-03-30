package util

import (
	"context"
	"os"
	"os/signal"

	"github.com/rs/zerolog/log"
)

func ContextWithSignal(parent context.Context, sig ...os.Signal) (context.Context, context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, sig...)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case sig := <-sigChan:
			log.Debug().
				Str("signal", sig.String()).
				Msg("received signal during execution")
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(sigChan)
		cancel()
	}
}
