package debug

import (
	"context"

	"github.com/airbloc/logger"
)

var log = logger.New("debug")

func ContextLifecycle(ctx context.Context, name string) {
	go func() {
		<-ctx.Done()
		log.Debug("context {} done", name)
	}()
}
