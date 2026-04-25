// Package eventbus is the in-process pub/sub layer for HyperGnomon
// subscriptions (DESIGN.md §5).
//
// Design notes:
//   - Publishers (flusher, fastsync, reorg detector) call Publish, which is
//     non-blocking: if the central ring is full, the event is dropped and
//     Dropped is incremented. This prevents the indexer from ever stalling
//     on a slow subscriber.
//   - One fan-out goroutine drains the central ring, evaluates each sub's
//     filter, and forwards matching events to per-sub channels. Per-sub
//     sends are also non-blocking: slow subs drop events and increment a
//     per-sub counter which the WS layer can poll and disconnect on.
//   - Close is idempotent and cancels every outstanding subscription.
//
// Bus itself has no dependency on storage or api packages to avoid import
// cycles. Events are defined as self-contained structs with typed payloads.
package eventbus

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// EventType enumerates the kinds of events the bus fans out.
type EventType uint8

const (
	EventInstall    EventType = 1 // new SC install durable
	EventInvoke     EventType = 2 // SC invocation durable
	EventVarChange  EventType = 3 // SC variables snapshot at height
	EventReorg      EventType = 4 // chain reorg detected (M2)
	EventSafe       EventType = 5 // safe_height advanced
	EventClassMatch EventType = 6 // SC classified (often fires with install)
)

// String returns a stable wire-level name for the event type.
func (t EventType) String() string {
	switch t {
	case EventInstall:
		return "install"
	case EventInvoke:
		return "invoke"
	case EventVarChange:
		return "var_change"
	case EventReorg:
		return "reorg"
	case EventSafe:
		return "safe"
	case EventClassMatch:
		return "class_match"
	default:
		return "unknown"
	}
}

// ParseEventType is the inverse of String — used when parsing subscribe
// params. Returns 0 for unknown names.
func ParseEventType(s string) EventType {
	switch s {
	case "install":
		return EventInstall
	case "invoke":
		return EventInvoke
	case "var_change":
		return EventVarChange
	case "reorg":
		return EventReorg
	case "safe":
		return EventSafe
	case "class_match":
		return EventClassMatch
	}
	return 0
}

// Event is the unit of publication. Payload is type-specific and the
// subscription layer is responsible for serializing it to JSON.
type Event struct {
	Type        EventType
	Height      int64
	SafeHeight  int64
	SCID        string
	Class       string
	Tags        []string
	Owner       string
	Sender      string
	Entrypoint  string
	Speculative bool
	Payload     any // type-specific; nil-tolerant
}

// Filter narrows which events a subscriber receives. All non-zero fields
// must match (AND). Events is a set of EventType; empty set = all types.
// CodeContains is held for M2+ (needs code cache); currently ignored.
// IncludeSpeculative opts the subscriber into mempool-derived events
// (Event.Speculative=true). Default false → speculative events are dropped
// so existing subscribers can't be surprised by unconfirmed data.
type Filter struct {
	Events             map[EventType]struct{}
	SCID               string
	Owner              string
	Sender             string
	Class              string
	CodeContains       string
	IncludeSpeculative bool
}

// Match reports whether the given event passes this filter.
func (f Filter) Match(e Event) bool {
	if e.Speculative && !f.IncludeSpeculative {
		return false
	}
	if len(f.Events) > 0 {
		if _, ok := f.Events[e.Type]; !ok {
			return false
		}
	}
	if f.SCID != "" && f.SCID != e.SCID {
		return false
	}
	if f.Owner != "" && f.Owner != e.Owner {
		return false
	}
	if f.Sender != "" && f.Sender != e.Sender {
		return false
	}
	if f.Class != "" && f.Class != e.Class {
		return false
	}
	return true
}

// subChanBuffer is the per-subscription channel depth. Larger = more
// tolerance for slow consumers, more memory. 256 is roughly one second of
// events at a heavy TELA install burst.
const subChanBuffer = 256

// subscription is the internal per-subscriber state. The RWMutex
// synchronizes close-vs-send so the fan-out goroutine never panics
// with "send on closed channel" when cancel races delivery.
// The send path takes RLock (many fan-outs concurrent); close takes
// a Lock that blocks until in-flight sends complete.
type subscription struct {
	id      string
	filter  Filter
	ch      chan Event
	mu      sync.RWMutex
	closed  bool
	dropped atomic.Int64
}

func (s *subscription) trySend(e Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- e:
	default:
		s.dropped.Add(1)
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// Bus is the in-process event fan-out. Construct with New, Run on a
// goroutine, Close to shut down.
type Bus struct {
	in     chan Event
	subs   sync.Map      // map[string]*subscription
	seq    atomic.Uint64 // subscription id generator
	closed atomic.Bool

	// Observability counters.
	PublishCount atomic.Int64
	DroppedCount atomic.Int64 // dropped at the central ring
}

// New creates a bus with the given central ring buffer. Typical value: 1024.
// If buffer <= 0, defaults to 1024.
func New(buffer int) *Bus {
	if buffer <= 0 {
		buffer = 1024
	}
	return &Bus{
		in: make(chan Event, buffer),
	}
}

// Publish enqueues an event for fan-out. Non-blocking: if the ring is
// full, DroppedCount is incremented and the event is discarded. Callers
// must never block on the bus — the indexer's hot path depends on it.
func (b *Bus) Publish(e Event) {
	if b.closed.Load() {
		return
	}
	select {
	case b.in <- e:
		b.PublishCount.Add(1)
	default:
		b.DroppedCount.Add(1)
	}
}

// Subscribe registers a new subscription. Returns the subscription id,
// the receive channel, and a cancel func. The channel is closed when the
// subscription is canceled or the bus is closed.
func (b *Bus) Subscribe(f Filter) (id string, events <-chan Event, cancel func()) {
	n := b.seq.Add(1)
	sub := &subscription{
		id:     "s_" + strconv.FormatUint(n, 36),
		filter: f,
		ch:     make(chan Event, subChanBuffer),
	}
	b.subs.Store(sub.id, sub)
	return sub.id, sub.ch, func() { b.cancel(sub) }
}

// cancel removes a subscription and closes its channel. Idempotent.
func (b *Bus) cancel(sub *subscription) {
	b.subs.Delete(sub.id)
	sub.close()
}

// Run is the fan-out loop. Call in a goroutine. Returns when the bus is
// closed.
func (b *Bus) Run() {
	for {
		e, ok := <-b.in
		if !ok {
			// Bus closed; cancel all subscriptions.
			b.subs.Range(func(_, v any) bool {
				b.cancel(v.(*subscription))
				return true
			})
			return
		}
		b.subs.Range(func(_, v any) bool {
			sub := v.(*subscription)
			if !sub.filter.Match(e) {
				return true
			}
			sub.trySend(e)
			return true
		})
	}
}

// Dropped returns the per-subscription drop count (events the bus
// discarded because the subscriber's channel was full).
func (b *Bus) Dropped(id string) int64 {
	v, ok := b.subs.Load(id)
	if !ok {
		return 0
	}
	return v.(*subscription).dropped.Load()
}

// SubscriberCount returns the number of active subscriptions.
func (b *Bus) SubscriberCount() int {
	n := 0
	b.subs.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// Close shuts down the bus. Idempotent. After Close, Publish is a no-op
// and Run returns.
func (b *Bus) Close() {
	if b.closed.Swap(true) {
		return
	}
	close(b.in)
}

// WaitForDrain blocks until the bus's central ring is empty or the timeout
// expires. Useful for tests that publish then want to verify delivery.
// Returns true if drained.
func (b *Bus) WaitForDrain(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(b.in) == 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return len(b.in) == 0
}
