package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// resetTELACount snapshots the global TELACount and restores it on cleanup,
// so tests that exercise applyClassifySeedCache stay order-independent.
func resetTELACount(t *testing.T) {
	t.Helper()
	prev := structures.TELACount.Load()
	t.Cleanup(func() { structures.TELACount.Store(prev) })
}

func TestClassifySeedRegistryHashIgnoresOrderAndCanExcludeNewerEntries(t *testing.T) {
	a := &registryEntry{scid: strings.Repeat("a", 64), owner: "owner-a", height: 10}
	b := &registryEntry{scid: strings.Repeat("b", 64), owner: "owner-b", height: 20}
	c := &registryEntry{scid: strings.Repeat("c", 64), owner: "owner-c", height: 30}

	h1 := classifySeedRegistryHash([]*registryEntry{b, c, a}, 20)
	h2 := classifySeedRegistryHash([]*registryEntry{a, b, c}, 20)
	// Real properties instead of a never-fail non-empty check: the hash of a
	// populated registry must differ from the empty-registry hash, and any
	// change to an entry's owner at the same height must change the hash.
	if empty := classifySeedRegistryHash(nil, 0); empty == h1 {
		t.Fatalf("hash(nil,0) == hash({a,b},20) = %q; registry contents are not hashed", h1)
	}
	if h1 != h2 {
		t.Fatalf("hash should ignore order: %q != %q", h1, h2)
	}
	ownerChanged := classifySeedRegistryHash([]*registryEntry{
		{scid: a.scid, owner: "owner-EVIL", height: a.height}, b,
	}, 20)
	if ownerChanged == h1 {
		t.Fatalf("changing an entry's owner at the same height did not change the hash: %q", h1)
	}

	withNewer := classifySeedRegistryHash([]*registryEntry{a, b, c}, 30)
	if withNewer == h1 {
		t.Fatalf("hash including newer entry should differ: %q", withNewer)
	}
}

func TestLoadMatchingClassifySeedCacheReturnsDeltaCandidates(t *testing.T) {
	dir := t.TempDir()
	a := &registryEntry{scid: strings.Repeat("a", 64), owner: "owner-a", height: 10}
	b := &registryEntry{scid: strings.Repeat("b", 64), owner: "owner-b", height: 20}
	c := &registryEntry{scid: strings.Repeat("c", 64), owner: "owner-c", height: 30}
	cache := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      "mainnet",
		GnomonSCID:   structures.GnomonSCID_Mainnet,
		RegistryHash: classifySeedRegistryHash([]*registryEntry{a, b}, 20),
		Height:       20,
		Classes: map[string]*structures.ClassMeta{
			a.scid: {Class: "TELA-INDEX-1", InstallHeight: 10, LastHeight: 20},
		},
		Variables: map[string]classifySeedVars{
			a.scid: {
				Height: 20,
				Vars: []*structures.SCIDVariable{
					{Key: "likes", Value: uint64(1)},
					{Key: "dislikes", Value: uint64(0)},
				},
			},
		},
	}
	if err := saveClassifySeedCache(dir, cache); err != nil {
		t.Fatalf("saveClassifySeedCache: %v", err)
	}

	got, delta, err := loadMatchingClassifySeedCache(dir, "mainnet", structures.GnomonSCID_Mainnet, []*registryEntry{c, b, a})
	if err != nil {
		t.Fatalf("loadMatchingClassifySeedCache: %v", err)
	}
	if got == nil {
		t.Fatal("expected matching cache")
	}
	if got.RegistryHash != cache.RegistryHash || got.Height != cache.Height {
		t.Fatalf("cache mismatch: got %+v want hash=%s height=%d", got, cache.RegistryHash, cache.Height)
	}
	if gotVars := got.Variables[a.scid]; gotVars.Height != 20 || len(gotVars.Vars) != 2 || gotVars.Vars[0].Key != "likes" {
		t.Fatalf("variable snapshot did not round-trip: %+v", gotVars)
	}
	if len(delta) != 1 || delta[0].scid != c.scid {
		t.Fatalf("delta = %+v, want only newer candidate c", delta)
	}
}

func TestApplyClassifySeedCacheSeedsStoreBuckets(t *testing.T) {
	resetTELACount(t)
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	indexSCID := strings.Repeat("1", 64)
	docSCID := strings.Repeat("2", 64)
	cache := &classifySeedCache{
		Version:    classifySeedCacheVersion,
		Network:    "mainnet",
		GnomonSCID: structures.GnomonSCID_Mainnet,
		Height:     700,
		IndexSCIDs: []string{indexSCID},
		DocSCIDs:   []string{docSCID},
		Classes: map[string]*structures.ClassMeta{
			indexSCID: {
				Class:         "TELA-INDEX-1",
				Tags:          []string{"all", "tela"},
				Name:          "Seeded App",
				DURL:          "seeded-app",
				Version:       "1.0.0",
				InstallHeight: 600,
				LastHeight:    700,
			},
			docSCID: {
				Class:         "TELA-DOC-1",
				Tags:          []string{"all", "tela"},
				Name:          "index.html",
				DURL:          "seeded-app",
				Version:       "1.0.0",
				InstallHeight: 601,
				LastHeight:    700,
			},
		},
		Variables: map[string]classifySeedVars{
			indexSCID: {
				Height: 700,
				Vars: []*structures.SCIDVariable{
					{Key: "likes", Value: uint64(1)},
					{Key: "dislikes", Value: uint64(0)},
					{Key: "var_header_name", Value: "Seeded App"},
				},
			},
			docSCID: {
				Height: 700,
				Vars: []*structures.SCIDVariable{
					{Key: "docVersion", Value: "1.0.0"},
					{Key: "var_header_name", Value: "index.html"},
				},
			},
		},
		Codes: map[string]classifySeedCode{
			docSCID: {Height: 601, Code: "Function Initialize() Uint64\n10 PRINT \"TELA-DOC-1\"\n20 RETURN 0\nEnd Function"},
		},
	}
	idx := &Indexer{
		DBDir:      t.TempDir(),
		Store:      store,
		CodePolicy: "tela",
	}

	if err := idx.applyClassifySeedCache(cache); err != nil {
		t.Fatalf("applyClassifySeedCache: %v", err)
	}

	installs, err := store.GetClassInstalls("TELA-INDEX-1", 0)
	if err != nil {
		t.Fatalf("GetClassInstalls: %v", err)
	}
	if len(installs) != 1 || installs[0].SCID != indexSCID {
		t.Fatalf("TELA-INDEX installs = %+v, want %s", installs, indexSCID)
	}
	vars, err := store.GetSCIDVariableDetailsAtHeight(indexSCID, 700)
	if err != nil {
		t.Fatalf("GetSCIDVariableDetailsAtHeight: %v", err)
	}
	if len(vars) != 3 {
		t.Fatalf("seeded vars len = %d, want 3: %+v", len(vars), vars)
	}
	code, err := store.GetSCCode(docSCID)
	if err != nil {
		t.Fatalf("GetSCCode: %v", err)
	}
	if code == nil || !strings.Contains(code.Code, "TELA-DOC-1") || code.InstallHeight != 601 {
		t.Fatalf("seeded code = %+v", code)
	}
	if structures.TELACount.Load() != 2 {
		t.Fatalf("TELACount = %d, want 2", structures.TELACount.Load())
	}
}

func TestNewClassifySeedCacheFromProbeCopiesBatches(t *testing.T) {
	indexSCID := strings.Repeat("3", 64)
	docSCID := strings.Repeat("4", 64)
	codeBatch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(codeBatch)
	codeBatch.AddSCCode(docSCID, 601, "Function Initialize() Uint64\n10 PRINT \"TELA-DOC-1\"\n20 RETURN 0\nEnd Function")
	varBatch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(varBatch)
	varBatch.AddClass(indexSCID, &structures.ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "Seeded",
		InstallHeight: 600,
		LastHeight:    700,
	})
	varBatch.AddVariables(indexSCID, 700, []*structures.SCIDVariable{
		{Key: "likes", Value: uint64(1)},
	})

	cache := newClassifySeedCacheFromProbe(
		"mainnet",
		structures.GnomonSCID_Mainnet,
		"registry-hash",
		700,
		[]string{indexSCID},
		[]string{docSCID},
		map[string][]string{"G45-NFT": {strings.Repeat("5", 64)}},
		codeBatch,
		varBatch,
	)

	if cache.Version != classifySeedCacheVersion || cache.Network != "mainnet" || cache.RegistryHash != "registry-hash" {
		t.Fatalf("cache identity mismatch: %+v", cache)
	}
	if got := cache.Classes[indexSCID]; got == nil || got.Class != "TELA-INDEX-1" || got.Name != "Seeded" {
		t.Fatalf("class meta missing from cache: %+v", got)
	}
	if got := cache.Variables[indexSCID]; got.Height != 700 || len(got.Vars) != 1 {
		t.Fatalf("vars missing from cache: %+v", got)
	}
	if got := cache.Codes[docSCID]; got.Height != 601 || !strings.Contains(got.Code, "TELA-DOC-1") {
		t.Fatalf("code missing from cache: %+v", got)
	}
	if got := cache.OtherClasses["G45-NFT"]; len(got) != 1 || got[0] != strings.Repeat("5", 64) {
		t.Fatalf("other classes missing from cache: %+v", cache.OtherClasses)
	}
}

func TestTryApplyClassifySeedCacheSeedsStoreAndReturnsDelta(t *testing.T) {
	resetTELACount(t)
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	oldSCID := strings.Repeat("6", 64)
	newSCID := strings.Repeat("7", 64)
	oldEntry := &registryEntry{scid: oldSCID, owner: "owner-old", height: 100}
	newEntry := &registryEntry{scid: newSCID, owner: "owner-new", height: 110}
	dir := t.TempDir()
	cache := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      "mainnet",
		GnomonSCID:   structures.GnomonSCID_Mainnet,
		RegistryHash: classifySeedRegistryHash([]*registryEntry{oldEntry}, 100),
		Height:       100,
		IndexSCIDs:   []string{oldSCID},
		Classes: map[string]*structures.ClassMeta{
			oldSCID: {Class: "TELA-INDEX-1", Tags: []string{"all", "tela"}, InstallHeight: 100, LastHeight: 100},
		},
		Variables: map[string]classifySeedVars{
			oldSCID: {Height: 100, Vars: []*structures.SCIDVariable{{Key: "likes", Value: uint64(1)}, {Key: "dislikes", Value: uint64(0)}}},
		},
	}
	if err := saveClassifySeedCache(dir, cache); err != nil {
		t.Fatalf("saveClassifySeedCache: %v", err)
	}
	idx := &Indexer{
		DBDir:                t.TempDir(),
		Store:                store,
		CodePolicy:           "tela",
		ClassifySeedCacheDir: dir,
	}

	delta, ok := idx.tryApplyClassifySeedCache("mainnet", structures.GnomonSCID_Mainnet, []*registryEntry{newEntry, oldEntry})
	if !ok {
		t.Fatal("tryApplyClassifySeedCache did not use matching cache")
	}
	if len(delta) != 1 || delta[0].scid != newSCID {
		t.Fatalf("delta = %+v, want new scid only", delta)
	}
	meta, err := store.GetSCIDClass(oldSCID)
	if err != nil {
		t.Fatalf("GetSCIDClass: %v", err)
	}
	if meta == nil || meta.Class != "TELA-INDEX-1" {
		t.Fatalf("seeded class = %+v, want TELA-INDEX-1", meta)
	}
}

// writeSeedCacheFile serializes a cache straight to disk, bypassing
// saveClassifySeedCache's defaulting and pruning, so rejection tests control
// the exact on-disk bytes.
func writeSeedCacheFile(t *testing.T, dir string, cache *classifySeedCache) {
	t.Helper()
	data, err := msgpack.Marshal(cache)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, classifySeedCacheFileName(cache)), data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// TestLoadMatchingClassifySeedCacheRejectsMutations table-tests every
// rejection gate in loadMatchingClassifySeedCache: a single field flipped on
// an otherwise-valid cache must make the load come back empty.
func TestLoadMatchingClassifySeedCacheRejectsMutations(t *testing.T) {
	a := &registryEntry{scid: strings.Repeat("a", 64), owner: "owner-a", height: 10}
	b := &registryEntry{scid: strings.Repeat("b", 64), owner: "owner-b", height: 20}
	candidates := []*registryEntry{a, b}

	validCache := func() *classifySeedCache {
		return &classifySeedCache{
			Version:      classifySeedCacheVersion,
			Network:      "mainnet",
			GnomonSCID:   structures.GnomonSCID_Mainnet,
			RegistryHash: classifySeedRegistryHash(candidates, 20),
			Height:       20,
			Classes: map[string]*structures.ClassMeta{
				a.scid: {Class: "TELA-INDEX-1", InstallHeight: 10, LastHeight: 20},
			},
		}
	}

	cases := []struct {
		name       string
		mutate     func(*classifySeedCache)
		candidates []*registryEntry
		garbage    bool
	}{
		{
			name:   "version mismatch",
			mutate: func(c *classifySeedCache) { c.Version = classifySeedCacheVersion + 1 },
		},
		{
			name:   "wrong network",
			mutate: func(c *classifySeedCache) { c.Network = "testnet" },
		},
		{
			name:   "wrong gnomon scid",
			mutate: func(c *classifySeedCache) { c.GnomonSCID = structures.GnomonSCID_Testnet },
		},
		{
			name:   "zero height",
			mutate: func(c *classifySeedCache) { c.Height = 0 },
		},
		{
			name: "owner changed at same height -> registry hash mismatch",
			candidates: []*registryEntry{
				{scid: a.scid, owner: "owner-EVIL", height: 10}, b,
			},
		},
		{
			name:    "garbage bytes",
			garbage: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.garbage {
				if err := os.WriteFile(filepath.Join(dir, "mainnet-v1-garbage-h20-feed.bin"),
					[]byte("\x00\x01not msgpack at all\xff\xfe"), 0o600); err != nil {
					t.Fatalf("WriteFile: %v", err)
				}
			} else {
				cache := validCache()
				if tc.mutate != nil {
					tc.mutate(cache)
				}
				writeSeedCacheFile(t, dir, cache)
			}

			loadWith := tc.candidates
			if loadWith == nil {
				loadWith = candidates
			}
			got, delta, err := loadMatchingClassifySeedCache(dir, "mainnet", structures.GnomonSCID_Mainnet, loadWith)
			if err != nil {
				t.Fatalf("loadMatchingClassifySeedCache: %v", err)
			}
			if got != nil {
				t.Fatalf("expected rejection, got cache %+v (delta %v)", got, delta)
			}
		})
	}

	// Control: the unmutated cache must load, or the table above proves nothing.
	t.Run("control accepts valid cache", func(t *testing.T) {
		dir := t.TempDir()
		writeSeedCacheFile(t, dir, validCache())
		got, _, err := loadMatchingClassifySeedCache(dir, "mainnet", structures.GnomonSCID_Mainnet, candidates)
		if err != nil {
			t.Fatalf("loadMatchingClassifySeedCache: %v", err)
		}
		if got == nil {
			t.Fatal("control cache rejected; rejection table is vacuous")
		}
	})
}

// TestLoadMatchingClassifySeedCachePicksNewestHeight writes two valid caches
// for the same registry lineage and asserts the higher-Height one wins.
func TestLoadMatchingClassifySeedCachePicksNewestHeight(t *testing.T) {
	dir := t.TempDir()
	a := &registryEntry{scid: strings.Repeat("a", 64), owner: "owner-a", height: 10}
	b := &registryEntry{scid: strings.Repeat("b", 64), owner: "owner-b", height: 20}
	c := &registryEntry{scid: strings.Repeat("c", 64), owner: "owner-c", height: 30}
	candidates := []*registryEntry{a, b, c}

	older := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      "mainnet",
		GnomonSCID:   structures.GnomonSCID_Mainnet,
		RegistryHash: classifySeedRegistryHash(candidates, 20),
		Height:       20,
	}
	newer := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      "mainnet",
		GnomonSCID:   structures.GnomonSCID_Mainnet,
		RegistryHash: classifySeedRegistryHash(candidates, 30),
		Height:       30,
	}
	writeSeedCacheFile(t, dir, older)
	writeSeedCacheFile(t, dir, newer)

	got, delta, err := loadMatchingClassifySeedCache(dir, "mainnet", structures.GnomonSCID_Mainnet, candidates)
	if err != nil {
		t.Fatalf("loadMatchingClassifySeedCache: %v", err)
	}
	if got == nil || got.Height != 30 {
		t.Fatalf("got %+v, want the height-30 cache", got)
	}
	if len(delta) != 0 {
		t.Fatalf("delta = %+v, want empty at the newest height", delta)
	}
}

// TestSaveClassifySeedCachePrunesOldSeeds saves three seeds for the same
// (network, gnomonSCID) pair and asserts only the newest two survive, while
// a different network's seed is untouched.
func TestSaveClassifySeedCachePrunesOldSeeds(t *testing.T) {
	dir := t.TempDir()

	otherNet := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      "testnet",
		GnomonSCID:   structures.GnomonSCID_Testnet,
		RegistryHash: strings.Repeat("e", 64),
		Height:       50,
	}
	if err := saveClassifySeedCache(dir, otherNet); err != nil {
		t.Fatalf("saveClassifySeedCache(testnet): %v", err)
	}

	for _, h := range []int64{100, 200, 300} {
		cache := &classifySeedCache{
			Version:      classifySeedCacheVersion,
			Network:      "mainnet",
			GnomonSCID:   structures.GnomonSCID_Mainnet,
			RegistryHash: fmt.Sprintf("%064d", h),
			Height:       h,
		}
		if err := saveClassifySeedCache(dir, cache); err != nil {
			t.Fatalf("saveClassifySeedCache(h=%d): %v", h, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var mainnet, testnet, tmps []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			tmps = append(tmps, name)
		case strings.HasPrefix(name, "mainnet-"):
			mainnet = append(mainnet, name)
		case strings.HasPrefix(name, "testnet-"):
			testnet = append(testnet, name)
		}
	}
	if len(tmps) != 0 {
		t.Fatalf("temp files left behind: %v", tmps)
	}
	if len(mainnet) != classifySeedCacheKeepCount {
		t.Fatalf("mainnet seeds = %v, want %d newest", mainnet, classifySeedCacheKeepCount)
	}
	joined := strings.Join(mainnet, ",")
	if !strings.Contains(joined, "-h200-") || !strings.Contains(joined, "-h300-") {
		t.Fatalf("surviving mainnet seeds = %v, want h200 + h300", mainnet)
	}
	if strings.Contains(joined, "-h100-") {
		t.Fatalf("oldest seed h100 not pruned: %v", mainnet)
	}
	if len(testnet) != 1 {
		t.Fatalf("testnet seeds = %v, want untouched single file", testnet)
	}
}

// TestTrueLocalCacheMissSeedGate covers audit #4: the cross-DB seed cache is
// consulted ONLY on a true local-cache miss; version-rejected or corrupt
// local caches must stay on the full-probe self-heal path.
func TestTrueLocalCacheMissSeedGate(t *testing.T) {
	resetTELACount(t)
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	scid := strings.Repeat("8", 64)
	entry := &registryEntry{scid: scid, owner: "owner-8", height: 100}
	dir := t.TempDir()
	cache := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      "mainnet",
		GnomonSCID:   structures.GnomonSCID_Mainnet,
		RegistryHash: classifySeedRegistryHash([]*registryEntry{entry}, 100),
		Height:       100,
		IndexSCIDs:   []string{scid},
		Classes: map[string]*structures.ClassMeta{
			scid: {Class: "TELA-INDEX-1", Tags: []string{"all", "tela"}, InstallHeight: 100, LastHeight: 100},
		},
	}
	if err := saveClassifySeedCache(dir, cache); err != nil {
		t.Fatalf("saveClassifySeedCache: %v", err)
	}
	idx := &Indexer{
		DBDir:                t.TempDir(),
		Store:                store,
		CodePolicy:           "tela",
		ClassifySeedCacheDir: dir,
	}

	// Local cache present-but-rejected: seed must NOT be applied even though
	// a matching seed file exists.
	if delta, ok := trueLocalCacheMissSeed(idx, false, "mainnet", structures.GnomonSCID_Mainnet, []*registryEntry{entry}); ok {
		t.Fatalf("seed applied on non-miss (delta %v); self-heal path was intercepted", delta)
	}

	// True miss: same seed must now be applied.
	if _, ok := trueLocalCacheMissSeed(idx, true, "mainnet", structures.GnomonSCID_Mainnet, []*registryEntry{entry}); !ok {
		t.Fatal("seed not applied on true local-cache miss")
	}
}

// TestShouldSaveSeedCacheGatingMatrix unit-tests the audit #1/#16 probe-tail
// gate: any single degradation signal must veto the cross-DB seed save.
func TestShouldSaveSeedCacheGatingMatrix(t *testing.T) {
	type args struct {
		closing        bool
		rpc1, rpc2     int64
		dec1, dec2     int64
		allowEarlyExit bool
		varFlushOK     bool
		registryHash   string
	}
	healthy := args{varFlushOK: true, registryHash: "abc123"}

	cases := []struct {
		name   string
		mutate func(*args)
		want   bool
	}{
		{name: "healthy probe saves", mutate: func(*args) {}, want: true},
		{name: "closing vetoes", mutate: func(a *args) { a.closing = true }, want: false},
		{name: "phase1 rpc errors veto", mutate: func(a *args) { a.rpc1 = 1 }, want: false},
		{name: "phase2 rpc errors veto", mutate: func(a *args) { a.rpc2 = 3 }, want: false},
		{name: "phase1 decode errors veto", mutate: func(a *args) { a.dec1 = 1 }, want: false},
		{name: "phase2 decode errors veto", mutate: func(a *args) { a.dec2 = 2 }, want: false},
		{name: "early-exit delta probe vetoes", mutate: func(a *args) { a.allowEarlyExit = true }, want: false},
		{name: "failed var flush vetoes", mutate: func(a *args) { a.varFlushOK = false }, want: false},
		{name: "empty registry hash vetoes", mutate: func(a *args) { a.registryHash = "" }, want: false},
		{name: "all signals degraded vetoes", mutate: func(a *args) {
			a.closing, a.rpc1, a.rpc2, a.dec1, a.dec2 = true, 1, 1, 1, 1
			a.allowEarlyExit, a.varFlushOK, a.registryHash = true, false, ""
		}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := healthy
			tc.mutate(&a)
			got := shouldSaveSeedCache(a.closing, a.rpc1, a.rpc2, a.dec1, a.dec2, a.allowEarlyExit, a.varFlushOK, a.registryHash)
			if got != tc.want {
				t.Fatalf("shouldSaveSeedCache(%+v) = %v, want %v", a, got, tc.want)
			}
		})
	}
}
