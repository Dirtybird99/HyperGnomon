package eventbus

import (
	"testing"
)

// BenchmarkFilter_Match_Speculative covers the new IncludeSpeculative
// early-return path added when the subscribe wire changed to honor
// include_speculative. Matters because Filter.Match is called once per
// event per subscriber in the bus fan-out — a regression here multiplies
// across the sub fleet.
func BenchmarkFilter_Match_Speculative(b *testing.B) {
	b.Run("speculative_event_opted_out", func(b *testing.B) {
		f := Filter{IncludeSpeculative: false}
		e := Event{Type: EventInstall, Speculative: true, SCID: "abc"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if f.Match(e) {
				b.Fatal("unexpected match")
			}
		}
	})
	b.Run("speculative_event_opted_in", func(b *testing.B) {
		f := Filter{IncludeSpeculative: true}
		e := Event{Type: EventInstall, Speculative: true, SCID: "abc"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !f.Match(e) {
				b.Fatal("expected match")
			}
		}
	})
	b.Run("finalized_event_default_filter", func(b *testing.B) {
		// Baseline: the no-speculative-no-opt case. Confirms the new
		// early-return branch didn't regress finalized event matching.
		f := Filter{}
		e := Event{Type: EventInstall, Speculative: false, SCID: "abc"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !f.Match(e) {
				b.Fatal("expected match")
			}
		}
	})
	b.Run("finalized_event_with_filters", func(b *testing.B) {
		// Full filter surface exercised: confirms the predicate chain
		// cost is dominated by the early returns, not the branch count.
		f := Filter{
			Events: map[EventType]struct{}{EventInstall: {}},
			SCID:   "abc",
			Owner:  "owner1",
			Class:  "TELA-INDEX-1",
		}
		e := Event{
			Type: EventInstall, SCID: "abc", Owner: "owner1",
			Class: "TELA-INDEX-1",
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !f.Match(e) {
				b.Fatal("expected match")
			}
		}
	})
}

// BenchmarkBus_PublishFanOut_Speculative covers end-to-end fan-out cost
// when half the subscribers are speculative-opted-in and half are not.
// Stresses the new filter-level drop so we can prove non-opted subs don't
// pay decode/channel cost for events they'll never see.
func BenchmarkBus_PublishFanOut_Speculative(b *testing.B) {
	const nSubs = 16
	bus := New(8192)
	go bus.Run()
	defer bus.Close()

	cancels := make([]func(), 0, nSubs)
	for i := 0; i < nSubs; i++ {
		f := Filter{IncludeSpeculative: i%2 == 0}
		_, ch, cancel := bus.Subscribe(f)
		cancels = append(cancels, cancel)
		// Drain receiver so channels don't back up.
		go func() {
			for range ch {
			}
		}()
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	ev := Event{Type: EventInstall, SCID: "abc", Speculative: true}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(ev)
	}
}
