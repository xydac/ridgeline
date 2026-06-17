package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestServeLoopRunsMultipleIterations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var count int32
	err := serveLoop(ctx, 10*time.Millisecond, func(ctx context.Context) {
		atomic.AddInt32(&count, 1)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&count); got < 3 {
		t.Fatalf("expected at least 3 iterations, got %d", got)
	}
}

func TestServeLoopExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after the first sync completes.
	var count int32
	done := make(chan struct{})
	go func() {
		_ = serveLoop(ctx, time.Hour, func(ctx context.Context) {
			if atomic.AddInt32(&count, 1) == 1 {
				cancel()
			}
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serveLoop did not exit after context cancellation")
	}
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Fatalf("expected exactly 1 iteration before cancel, got %d", got)
	}
}
