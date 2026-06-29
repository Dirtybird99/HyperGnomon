package storage

import (
	"reflect"
	"sort"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// Backend conformance suite. Every Storage backend (bbolt today; sqlite when it
// lands) must pass this same set of behavioral checks against the public Storage
// interface — it is the gate that defines "a correct backend". Assertions sort
// where read order is not part of the contract, so a backend that iterates in a
// different key order (e.g. SQL) can still pass.
//
// Add a backend by appending one factory to backendFactories(); the whole suite
// then runs against it automatically.
//
// SCOPE (honest, as of today): this suite gates round-trip correctness + Route-B
// containment. It does NOT yet assert atomicity-under-failure (a partial
// FlushBatch must not commit some buckets), the EnableSync/DisableSync durability
// flip, legacy-msgpack read compatibility (tag-byte dispatch on existing DBs),
// re-index dedup across heights, or large-scan correctness. These must be added
// before the suite can certify a SECOND backend (e.g. sqlite) — passing it today
// is necessary but not sufficient for backend equivalence.

type backendFactory struct {
	name string
	open func(testing.TB) Storage
}

func backendFactories() []backendFactory {
	return []backendFactory{
		{"bbolt", func(tb testing.TB) Storage { return openTestStore(tb) }},
		// {"sqlite", func(tb testing.TB) Storage { return openTestSQLite(tb) }}, // Phase 2
	}
}

func TestStorageConformance(t *testing.T) {
	for _, f := range backendFactories() {
		t.Run(f.name, func(t *testing.T) {
			t.Run("OwnersAndIndexHeight", func(t *testing.T) { conformanceOwners(t, f.open) })
			t.Run("FlushBatchRoundTrip", func(t *testing.T) { conformanceFlushRoundTrip(t, f.open) })
			t.Run("RouteB", func(t *testing.T) { conformanceRouteB(t, f.open) })
			t.Run("NonBatchWrites", func(t *testing.T) { conformanceNonBatch(t, f.open) })
		})
	}
}

func conformanceOwners(t *testing.T, open func(testing.TB) Storage) {
	s := open(t)

	if err := s.StoreLastIndexHeight(41); err != nil {
		t.Fatalf("StoreLastIndexHeight: %v", err)
	}
	if h, err := s.GetLastIndexHeight(); err != nil || h != 41 {
		t.Fatalf("GetLastIndexHeight = %d, %v; want 41", h, err)
	}

	owners := map[string]string{
		"aa" + randHex(31): "dero1qowner1",
		"bb" + randHex(31): "dero1qowner2",
		"cc" + randHex(31): "dero1qowner1", // owner1 owns two SCIDs
	}
	b := NewWriteBatch()
	for scid, owner := range owners {
		b.AddOwner(scid, owner)
	}
	b.LastHeight = 100
	if err := s.FlushBatch(b); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(b)

	for scid, want := range owners {
		if got, err := s.GetOwner(scid); err != nil || got != want {
			t.Errorf("GetOwner(%s) = %q, %v; want %q", scid, got, err, want)
		}
	}
	all, err := s.GetAllOwnersAndSCIDs()
	if err != nil || !reflect.DeepEqual(all, owners) {
		t.Errorf("GetAllOwnersAndSCIDs = %v, %v; want %v", all, err, owners)
	}
	if n, err := s.GetSCIDCount(); err != nil || n != int64(len(owners)) {
		t.Errorf("GetSCIDCount = %d, %v; want %d", n, err, len(owners))
	}
	if got := sortedKeys(all); !reflect.DeepEqual(mustSCIDs(t, s), got) {
		t.Errorf("GetAllSCIDs = %v; want %v", mustSCIDs(t, s), got)
	}
	// owner1 owns exactly its two SCIDs (reverse index via AddOwner).
	want1 := []string{}
	for scid, owner := range owners {
		if owner == "dero1qowner1" {
			want1 = append(want1, scid)
		}
	}
	sort.Strings(want1)
	got1, err := s.GetSCIDsByOwner("dero1qowner1")
	sort.Strings(got1)
	if err != nil || !reflect.DeepEqual(got1, want1) {
		t.Errorf("GetSCIDsByOwner(owner1) = %v, %v; want %v", got1, err, want1)
	}
	if h, err := s.GetLastIndexHeight(); err != nil || h != 100 {
		t.Errorf("GetLastIndexHeight after batch = %d, %v; want 100", h, err)
	}
}

func conformanceFlushRoundTrip(t *testing.T, open func(testing.TB) Storage) {
	s := open(t)
	scid := "de" + randHex(31)
	b := NewWriteBatch()
	b.AddOwner(scid, "dero1qowner")

	// Two invocations.
	for i := 0; i < 2; i++ {
		b.AddInvocation(structures.InvokeRecord{
			Scid: scid, Sender: "dero1qsender", Entrypoint: "Transfer", Height: int64(10 + i),
			Details: &structures.SCTXParse{
				Txid: randHex(32), Scid: scid, Entrypoint: "Transfer",
				Method: structures.MethodInvokeSC, Sender: "dero1qsender",
				Fees: uint64(7 + i), Height: int64(10 + i),
			},
		})
	}
	// Variable snapshots at two heights + interaction heights.
	vars := []*structures.SCIDVariable{{Key: "balance", Value: uint64(42)}, {Key: "name", Value: "TELA"}}
	b.AddVariables(scid, 10, vars)
	b.AddVariables(scid, 20, []*structures.SCIDVariable{{Key: "balance", Value: uint64(99)}})
	b.AddInteractionHeight(scid, 10)
	b.AddInteractionHeight(scid, 20)
	b.RegTxCount, b.BurnTxCount, b.NormTxCount = 5, 1, 3
	if err := s.FlushBatch(b); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(b)

	inv, err := s.GetInvokeDetailsBySCID(scid)
	if err != nil || len(inv) != 2 {
		t.Fatalf("GetInvokeDetailsBySCID = %d records, %v; want 2", len(inv), err)
	}
	got, err := s.GetSCIDVariableDetailsAtHeight(scid, 10)
	if err != nil || len(got) != len(vars) {
		t.Fatalf("GetSCIDVariableDetailsAtHeight(10) = %d vars, %v; want %d", len(got), err, len(vars))
	}
	for i := range vars {
		if got[i].Key != vars[i].Key || !reflect.DeepEqual(got[i].Value, vars[i].Value) {
			t.Errorf("var[%d] = {%v:%v}; want {%v:%v}", i, got[i].Key, got[i].Value, vars[i].Key, vars[i].Value)
		}
	}
	heights, err := s.GetSCIDInteractionHeights(scid)
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	if err != nil || !reflect.DeepEqual(heights, []int64{10, 20}) {
		t.Errorf("GetSCIDInteractionHeights = %v, %v; want [10 20]", heights, err)
	}
	if reg, burn, norm, err := s.GetTxCounts(); err != nil || reg != 5 || burn != 1 || norm != 3 {
		t.Errorf("GetTxCounts = (%d,%d,%d), %v; want (5,1,3)", reg, burn, norm, err)
	}
}

func conformanceRouteB(t *testing.T, open func(testing.TB) Storage) {
	s := open(t)
	scid := "c1" + randHex(31)
	addr := "dero1q" + randHex(30)
	hash := randHex(32)
	const h = int64(6927000)

	b := NewWriteBatch()
	b.AddOwner(scid, addr)
	b.AddClass(scid, &structures.ClassMeta{Class: "TELA-INDEX-1", Name: "MyTELA", InstallHeight: h, LastHeight: h})
	b.AddInstall(scid, h, &structures.InstallRecord{Owner: addr, Entrypoint: "InitializePrivate", Fees: 1234})
	b.AddSCCode(scid, h, "Function Initialize() Uint64 10 RETURN 0 End Function")
	b.AddAddrSCID(addr, scid, h)
	b.AddBlockHash(h, hash)
	if err := s.FlushBatch(b); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(b)

	if meta, err := s.GetSCIDClass(scid); err != nil || meta == nil || meta.Class != "TELA-INDEX-1" {
		t.Errorf("GetSCIDClass = %+v, %v; want Class=TELA-INDEX-1", meta, err)
	}
	if ci, err := s.GetClassInstalls("TELA-INDEX-1", 10); err != nil || !containsSCID(ci, scid) {
		t.Errorf("GetClassInstalls(TELA-INDEX-1) = %v, %v; want to contain %s", ci, err, scid)
	}
	if ci, err := s.GetInstallsInRange(h-1, h+1, 10); err != nil || !containsSCID(ci, scid) {
		t.Errorf("GetInstallsInRange = %v, %v; want to contain %s", ci, err, scid)
	}
	if code, err := s.GetSCCode(scid); err != nil || code == nil || code.Code == "" {
		t.Errorf("GetSCCode = %+v, %v; want non-empty code", code, err)
	}
	as, err := s.GetAddressSCIDs(addr)
	if err != nil || as[scid] == nil || as[scid].Count != 1 {
		t.Errorf("GetAddressSCIDs(addr)[scid] = %+v, %v; want Count=1", as[scid], err)
	}
	if got, err := s.GetBlockHash(h); err != nil || got != hash {
		t.Errorf("GetBlockHash(%d) = %q, %v; want %q", h, got, err, hash)
	}
}

func conformanceNonBatch(t *testing.T, open func(testing.TB) Storage) {
	s := open(t)
	scid := "ab" + randHex(31)

	if err := s.StoreOwner(scid, "dero1qdirect"); err != nil {
		t.Fatalf("StoreOwner: %v", err)
	}
	if got, err := s.GetOwner(scid); err != nil || got != "dero1qdirect" {
		t.Errorf("GetOwner(direct) = %q, %v; want dero1qdirect", got, err)
	}
	if err := s.PutSCCode(scid, &structures.SCCodeEntry{Code: "Function X() Uint64 RETURN 0 End Function", InstallHeight: 5}); err != nil {
		t.Fatalf("PutSCCode: %v", err)
	}
	if code, err := s.GetSCCode(scid); err != nil || code == nil || code.InstallHeight != 5 {
		t.Errorf("GetSCCode(direct) = %+v, %v; want InstallHeight=5", code, err)
	}
	if err := s.StoreInvalidSCIDDeploys(scid, 99); err != nil {
		t.Fatalf("StoreInvalidSCIDDeploys: %v", err)
	}
	if m, err := s.GetInvalidSCIDDeploys(); err != nil || m[scid] != 99 {
		t.Errorf("GetInvalidSCIDDeploys()[scid] = %d, %v; want 99", m[scid], err)
	}
}

// --- small assertion helpers ---

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func mustSCIDs(t *testing.T, s Storage) []string {
	t.Helper()
	scids, err := s.GetAllSCIDs()
	if err != nil {
		t.Fatalf("GetAllSCIDs: %v", err)
	}
	sort.Strings(scids)
	return scids
}

func containsSCID(installs []structures.ClassInstall, scid string) bool {
	for _, ci := range installs {
		if ci.SCID == scid {
			return true
		}
	}
	return false
}
