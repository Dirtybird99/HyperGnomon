package indexer

import "time"

// sleepQuick is factored out so tests can override with a faster tick
// if they want to drive the field-refresh goroutine deterministically.
// Default is 100 ms — fine for operational use, swap in tests as
// needed.
var sleepQuick = func() { time.Sleep(100 * time.Millisecond) }
