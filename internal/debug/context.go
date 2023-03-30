package debug

import (
	"context"

	"github.com/rs/zerolog/log"
)

func ContextLifecycle(ctx context.Context, name string) {
	go func() {
		<-ctx.Done()
		log.Debug().
			Str("name", name).
			Msg("context done")
	}()
}
