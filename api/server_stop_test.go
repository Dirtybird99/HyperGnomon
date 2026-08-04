package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func newStopTestStore(t *testing.T) *storage.BboltStore {
	t.Helper()
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// freeLoopbackAddr reserves an ephemeral port and releases it so the Server
// under test can bind it. The gap between Close and rebind is racy in
// principle but private to the test host in practice.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestServerStop_UnblocksStartAndFreesPort(t *testing.T) {
	store := newStopTestStore(t)
	addr := freeLoopbackAddr(t)
	s := NewServer(store, nil, addr, nil, nil, nil, nil, 1024)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Start() }()

	waitFor(t, 5*time.Second, func() bool {
		resp, err := http.Get("http://" + addr + "/api/getinfo")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return true
	}, "server to accept requests")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start returned %v after clean Stop, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port not released after Stop: %v", err)
	}
	_ = ln.Close()
}

func TestServerStop_BeforeStartRefusesLaterStart(t *testing.T) {
	store := newStopTestStore(t)
	bus := eventbus.New(16)
	go bus.Run()
	t.Cleanup(bus.Close)
	s := NewServer(store, nil, "127.0.0.1:0", nil, nil, bus, nil, 1024)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.Start() }()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Start after Stop returned %v, want http.ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start after Stop did not return")
	}

	if n := bus.SubscriberCount(); n != 0 {
		t.Fatalf("refused Start leaked a bus subscription (count=%d)", n)
	}
}

func TestServerStop_Idempotent(t *testing.T) {
	store := newStopTestStore(t)
	s := NewServer(store, nil, "127.0.0.1:0", nil, nil, nil, nil, 1024)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Start() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

func TestServerStop_ConcurrentWithStart(t *testing.T) {
	store := newStopTestStore(t)
	for i := 0; i < 50; i++ {
		s := NewServer(store, nil, "127.0.0.1:0", nil, nil, nil, nil, 1024)
		errCh := make(chan error, 1)
		go func() { errCh <- s.Start() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.Stop(ctx); err != nil {
			cancel()
			t.Fatalf("iter %d: Stop: %v", i, err)
		}
		cancel()

		select {
		case err := <-errCh:
			// nil: Stop drained a started server. ErrServerClosed: Stop won
			// the race and Start refused to serve. Both are clean exits.
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("iter %d: Start returned %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: Start did not return after Stop", i)
		}
	}
}

func TestRunTELAInvalidator_ProcessesEventsAndExitsOnStopCh(t *testing.T) {
	store := newStopTestStore(t)
	bus := eventbus.New(16)
	go bus.Run()
	t.Cleanup(bus.Close)

	const scid = "aabbccdd"
	s := &Server{store: store, bus: bus, telaCache: newTELAContentCache(1024)}
	s.telaCache.Put(scid+"|index.html", &structures.TELAContentEntry{Body: []byte("x")})

	stopCh := make(chan struct{})
	done := make(chan struct{})
	s.wg.Add(1)
	go func() {
		s.runTELAInvalidator(stopCh)
		close(done)
	}()

	waitFor(t, 2*time.Second, func() bool { return bus.SubscriberCount() == 1 }, "invalidator to subscribe")

	bus.Publish(eventbus.Event{Type: eventbus.EventInstall, SCID: scid})
	waitFor(t, 2*time.Second, func() bool {
		return s.telaCache.Get(scid+"|index.html") == nil
	}, "install event to invalidate the cache entry")

	close(stopCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("invalidator did not exit on stopCh close")
	}
	waitFor(t, 2*time.Second, func() bool { return bus.SubscriberCount() == 0 }, "subscription to be canceled")
}

func TestRunTELAInvalidator_ExitsOnBusClose(t *testing.T) {
	store := newStopTestStore(t)
	bus := eventbus.New(16)
	go bus.Run()

	s := &Server{store: store, bus: bus, telaCache: newTELAContentCache(1024)}

	stopCh := make(chan struct{})
	done := make(chan struct{})
	s.wg.Add(1)
	go func() {
		s.runTELAInvalidator(stopCh)
		close(done)
	}()

	waitFor(t, 2*time.Second, func() bool { return bus.SubscriberCount() == 1 }, "invalidator to subscribe")

	bus.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("invalidator did not exit on bus close")
	}
}
