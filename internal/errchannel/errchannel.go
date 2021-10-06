package errchannel

import "go.uber.org/atomic"

// ErrChannel wraps an error channel. It protect the channel from closing in a race condition / writing after close.
type ErrChannel struct {
	channel chan error
	closed  atomic.Bool
}

func New() *ErrChannel {
	return &ErrChannel{
		channel: make(chan error, 1),
	}
}

func (e *ErrChannel) Send(err error) {
	if e.closed.Load() {
		return
	}
	select {
	case e.channel <- err:
	default:
	}
}

func (e *ErrChannel) Recv() <-chan error {
	return e.channel
}

func (e *ErrChannel) Close() {
	if swapped := e.closed.CAS(false, true); swapped {
		close(e.channel)
	}
}
