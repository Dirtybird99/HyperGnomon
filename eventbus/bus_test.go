package eventbus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBus_PublishSubscribe(t *testing.T) {
	b := New(16)
	go b.Run()
	defer b.Close()

	_, ch, cancel := b.Subscribe(Filter{})
	defer cancel()

	b.Publish(Event{Type: EventInstall, SCID: "abc", Height: 100})

	select {
	case e := <-ch:
		if e.Type != EventInstall || e.SCID != "abc" || e.Height != 100 {
			t.Fatalf("unexpected event: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("event not received within 1s")
	}
}

func TestBus_FilterByType(t *testing.T) {
	b := New(16)
	go b.Run()
	defer b.Close()

	_, ch, cancel := b.Subscribe(Filter{
		Events: map[EventType]struct{}{EventInstall: {}},
	})
	defer cancel()

	b.Publish(Event{Type: EventInvoke, SCID: "nope"})
	b.Publish(Event{Type: EventInstall, SCID: "yes"})
	b.Publish(Event{Type: EventInvoke, SCID: "nope"})
	b.WaitForDrain(time.Second)

	// Should receive exactly the install; after that channel is empty.
	select {
	case e := <-ch:
		if e.SCID != "yes" {
			t.Fatalf("expected install event SCID 'yes', got %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("install event not received")
	}

	select {
	case e := <-ch:
		t.Fatalf("expected no more events, got %+v", e)
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

func TestBus_FilterBySCID(t *testing.T) {
	b := New(16)
	go b.Run()
	defer b.Close()

	_, ch, cancel := b.Subscribe(Filter{SCID: "want"})
	defer cancel()

	b.Publish(Event{Type: EventInvoke, SCID: "other"})
	b.Publish(Event{Type: EventInvoke, SCID: "want"})
	b.WaitForDrain(time.Second)

	select {
	case e := <-ch:
		if e.SCID != "want" {
			t.Fatalf("expected SCID 'want', got %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("matching event not received")
	}
}

func TestBus_MultipleSubs(t *testing.T) {
	b := New(16)
	go b.Run()
	defer b.Close()

	_, chAll, cancelAll := b.Subscribe(Filter{})
	defer cancelAll()
	_, chInvoke, cancelInvoke := b.Subscribe(Filter{
		Events: map[EventType]struct{}{EventInvoke: {}},
	})
	defer cancelInvoke()

	b.Publish(Event{Type: EventInstall, SCID: "a"})
	b.Publish(Event{Type: EventInvoke, SCID: "b"})
	b.WaitForDrain(time.Second)

	// chAll should get both.
	for i := 0; i < 2; i++ {
		select {
		case <-chAll:
		case <-time.After(time.Second):
			t.Fatalf("chAll missing event %d", i)
		}
	}

	// chInvoke should get only one.
	select {
	case e := <-chInvoke:
		if e.Type != EventInvoke {
			t.Fatalf("chInvoke got wrong type: %v", e.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("chInvoke missing invoke event")
	}
	select {
	case e := <-chInvoke:
		t.Fatalf("chInvoke got extra event: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBus_SpeculativeOptIn(t *testing.T) {
	b := New(16)
	go b.Run()
	defer b.Close()

	_, chDefault, cancelDefault := b.Subscribe(Filter{})
	defer cancelDefault()
	_, chSpec, cancelSpec := b.Subscribe(Filter{IncludeSpeculative: true})
	defer cancelSpec()

	b.Publish(Event{Type: EventInstall, SCID: "sp", Speculative: true})
	b.Publish(Event{Type: EventInstall, SCID: "fin", Speculative: false})
	b.WaitForDrain(time.Second)

	// Default sub: only the finalized event.
	seenDefault := drain(chDefault, 200*time.Millisecond)
	if len(seenDefault) != 1 || seenDefault[0].SCID != "fin" {
		t.Fatalf("default sub expected [fin], got %+v", seenDefault)
	}

	// Opt-in sub: both.
	seenSpec := drain(chSpec, 200*time.Millisecond)
	if len(seenSpec) != 2 {
		t.Fatalf("speculative sub expected 2 events, got %d: %+v", len(seenSpec), seenSpec)
	}
}

// drain reads all events available on ch within d. Used by tests that need
// to assert "at most N events" with an upper time bound.
func drain(ch <-chan Event, d time.Duration) []Event {
	deadline := time.After(d)
	var out []Event
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
}

func TestBus_CancelClosesChannel(t *testing.T) {
	b := New(16)
	go b.Run()
	defer b.Close()

	_, ch, cancel := b.Subscribe(Filter{})
	cancel()

	// After cancel, the channel should be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel, got value")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}
}

func TestBus_SlowConsumerDrops(t *testing.T) {
	// Central ring large enough to never drop; we want to force per-sub drops.
	b := New(subChanBuffer * 4)
	go b.Run()
	defer b.Close()

	id, _, cancel := b.Subscribe(Filter{})
	defer cancel()

	// Publish more than subChanBuffer without reading — force per-sub drops.
	for i := 0; i < subChanBuffer+100; i++ {
		b.Publish(Event{Type: EventInstall, Height: int64(i)})
	}
	b.WaitForDrain(2 * time.Second)
	// Give fan-out a moment to process everything drained from the ring.
	time.Sleep(50 * time.Millisecond)

	if b.Dropped(id) == 0 {
		t.Fatalf("expected non-zero drops for slow subscriber (central dropped=%d)", b.DroppedCount.Load())
	}
}

func TestBus_Close(t *testing.T) {
	b := New(16)
	go b.Run()

	_, ch, _ := b.Subscribe(Filter{})

	b.Close()
	// Idempotent: second Close must not panic.
	b.Close()

	// Channel should be closed by cancel-all-on-close path.
	select {
	case _, ok := <-ch:
		if ok {
			// Might get a value that was mid-flight; that's fine as long
			// as subsequent reads return closed.
			_, ok2 := <-ch
			if ok2 {
				t.Fatal("expected channel closed after Close")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after bus Close")
	}
}

func TestBus_Concurrent(t *testing.T) {
	b := New(256)
	go b.Run()
	defer b.Close()

	const nSubs = 8
	const nPublishers = 4
	const perPublisher = 200

	// Use atomic.Int64 so increments are race-safe under -race without a mutex.
	counters := make(map[string]*atomic.Int64, nSubs)
	var countersMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < nSubs; i++ {
		id, ch, cancel := b.Subscribe(Filter{})
		c := new(atomic.Int64)
		countersMu.Lock()
		counters[id] = c
		countersMu.Unlock()
		wg.Add(1)
		go func(ch <-chan Event, c *atomic.Int64) {
			defer wg.Done()
			defer cancel()
			for range ch {
				c.Add(1)
			}
		}(ch, c)
	}

	// Publishers
	var pubWG sync.WaitGroup
	for i := 0; i < nPublishers; i++ {
		pubWG.Add(1)
		go func(id int) {
			defer pubWG.Done()
			for j := 0; j < perPublisher; j++ {
				b.Publish(Event{Type: EventInvoke, Height: int64(id*perPublisher + j)})
			}
		}(i)
	}
	pubWG.Wait()
	b.WaitForDrain(2 * time.Second)
	time.Sleep(100 * time.Millisecond)

	total := nPublishers * perPublisher
	countersMu.Lock()
	defer countersMu.Unlock()
	for id, c := range counters {
		got := c.Load()
		if got == 0 {
			t.Errorf("sub %s got 0 events, expected up to %d", id, total)
		}
	}
}
