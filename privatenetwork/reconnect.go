package privatenetwork

import (
	"context"
	"time"
)

// requestStreamRecovery is called on stream loss/open failure, never by a
// synchronization timer. Coalesce failures across peers into one recovery job.
func (h *Host) requestStreamRecovery() {
	if h.ctx.Err() != nil {
		return
	}
	select {
	case h.recoverStream <- struct{}{}:
	default:
	}
}

func (h *Host) recoverSubscriptions() {
	defer h.recovery.Done()
	delay := time.Second
	var lastAttempt time.Time
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.recoverStream:
		}
		if time.Since(lastAttempt) > time.Minute {
			delay = time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-h.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		lastAttempt = time.Now()
		ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
		err := h.Reconnect(ctx)
		cancel()
		if err != nil {
			h.requestStreamRecovery()
		}
		// Failed asynchronous stream opens also enqueue recovery. The delay grows
		// across those attempts even if headsync returned nil (not a delivery ack).
		delay = min(delay*2, 30*time.Second)
	}
}
