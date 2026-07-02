package indexer

import (
	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/storage"
)

// publishBatchEvents fans out events for everything that just committed.
// Called inside flusherLoop ONLY on FlushBatch success — subscribers
// never see events the DB doesn't already carry.
//
// Bus is nil-safe: library embeddings that don't subscribe get no overhead
// beyond the nil check.
func (idx *Indexer) publishBatchEvents(b *storage.WriteBatch, newHeight, safeHeight, prevSafe int64) {
	bus := idx.Bus
	if bus == nil || b == nil {
		return
	}

	// Installs — one event per new SC deployment.
	for mapKey, rec := range b.Installs {
		scid, h, ok := splitSCIDHeightKey(mapKey)
		if !ok {
			continue
		}
		bus.Publish(eventbus.Event{
			Type:       eventbus.EventInstall,
			Height:     h,
			SafeHeight: safeHeight,
			SCID:       scid,
			Owner:      rec.Owner,
			Entrypoint: rec.Entrypoint,
			Payload:    rec,
		})
	}

	// Invocations.
	for _, inv := range b.Invocations {
		bus.Publish(eventbus.Event{
			Type:       eventbus.EventInvoke,
			Height:     inv.Height,
			SafeHeight: safeHeight,
			SCID:       inv.Scid,
			Sender:     inv.Sender,
			Entrypoint: inv.Entrypoint,
			Payload:    inv.Details,
		})
	}

	// Variable snapshots — one event per (scid, height), which is exactly one
	// entry of the flat Variables map.
	for k, vars := range b.Variables {
		bus.Publish(eventbus.Event{
			Type:       eventbus.EventVarChange,
			Height:     k.Height,
			SafeHeight: safeHeight,
			SCID:       k.Scid,
			Payload:    vars,
		})
	}

	// Class assignments — one event per SCID whose class metadata was
	// written or refreshed. Skip UNKNOWN to cut noise; class_match is
	// meant for "this is a TELA/NFA/G45/…" subscribers.
	for scid, meta := range b.Classes {
		if meta == nil || meta.Class == "" || meta.Class == "UNKNOWN" {
			continue
		}
		bus.Publish(eventbus.Event{
			Type:       eventbus.EventClassMatch,
			Height:     meta.LastHeight,
			SafeHeight: safeHeight,
			SCID:       scid,
			Class:      meta.Class,
			Tags:       meta.Tags,
			Payload:    meta,
		})
	}

	// SafeHeight advanced: one event per advance, carrying the old+new.
	// Fires even on the first flush when prevSafe is 0 — clients gate on
	// the new value, not the delta.
	if safeHeight > prevSafe {
		bus.Publish(eventbus.Event{
			Type:       eventbus.EventSafe,
			Height:     newHeight,
			SafeHeight: safeHeight,
			Payload: map[string]int64{
				"prev_safe_height": prevSafe,
				"safe_height":      safeHeight,
				"tip":              newHeight,
			},
		})
	}
}

// splitSCIDHeightKey inverts scidHeightKey from storage/storage.go.
// Duplicated here (rather than exported) because it's an internal wire
// format between WriteBatch producers and the publish site.
func splitSCIDHeightKey(k string) (scid string, height int64, ok bool) {
	at := -1
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '@' {
			at = i
			break
		}
	}
	if at < 0 {
		return "", 0, false
	}
	// Parse height from decimal ascii.
	var n int64
	for i := at + 1; i < len(k); i++ {
		c := k[i]
		if c < '0' || c > '9' {
			return "", 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return k[:at], n, true
}
