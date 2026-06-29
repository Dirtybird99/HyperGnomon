# The Gnomon Ecosystem — A Storage & Integration Study

> Status: durable reference. Last verified against upstream sources 2026-06-24.
> Scope: how the DERO "Gnomon" indexer family stores data and how consumers embed it,
> read against HyperGnomon's storage design choices (bbolt-only, BE8 height-prefixed
> ordered keys, per-row merge-on-write, sqlite deferred, graviton rejected).

HyperGnomon descends from `civilware/Gnomon`, the canonical DERO smart-contract indexer
("decentralized search engine"). Before committing to a bbolt-only storage core we walked
the full fork tree and the in-the-wild consumer set to answer two questions honestly:

1. **Did anyone ship a SQLite gnomon, and does it beat KV on the real schema?** (tests our *defer-sqlite* call)
2. **Is graviton ever used for the ordered prefix/range scans we need?** (tests our *reject-graviton* call)

The short answer: a SQLite gnomon **does** ship (`siteraiser/simple-gnomon`), but its scope and
write model do not reproduce HyperGnomon's merge-heavy schema, so it neither vindicates nor
refutes sqlite on a like-for-like basis. And graviton, where it is used in production
(`HOLOGRAM`), is a **point-get versioned content cache** — never an ordered index. Both
HyperGnomon calls survive inspection, with one nuance worth recording (see §5).

---

## 1. Ecosystem map

Recursive fork lineage:
`civilware/Gnomon` (bbolt + graviton) → `secretnamebasis/simple-gnomon` (bbolt rewrite) → `siteraiser/simple-gnomon` (SQLite reimplementation).

| Project | Role | Storage engine(s) | Activity | Gnomon integration |
|---|---|---|---|---|
| **civilware/Gnomon** | Canonical baseline indexer | bbolt (default) **+** graviton, one interface, same logical layout | Go 1.18; deps pinned 2022–2023; mature/slow-moving | Is the original |
| **secretnamebasis/simple-gnomon** | "Simplified" fork for `simple-wallet` | bbolt-only (v1.3.7); graviton only via `original` branch / transitive derohe | Active; last push 2026-04-16; v3.0.0–v3.9.0; MIT | Forked from civilware |
| **siteraiser/simple-gnomon** | **SQLite reimplementation** ("SQLITE implementation of GNOMON") | SQLite via cgo `mattn/go-sqlite3` v1.14.33 | Active; pushed 2026-02-27; "Simple Gnomon Lite" release 2026-02-09; MIT | Fork-of-fork (child of secretnamebasis) |
| **moralpriest/Gnomon** | Thin maintenance fork | bbolt + graviton (verbatim upstream) | Pushed 2026-06-02 but **+1 trivial commit** (a log-spam move) | Fork of civilware |
| **Azylem/Gnomon** | Stale mirror | bbolt + graviton (verbatim) | Frozen at civilware `ce6788a` (Nov 2023); 0 ahead | Fork of civilware |
| **SixofClubsss/Gnomon** | Stale mirror | bbolt + graviton (verbatim) | Frozen at `ce6788a` (Nov 2023); 0 ahead | Fork of civilware |
| **dReam-dApps/dReams** (`gnomes/`) | Consumer wrapper (the canonical `StartGnomon` embed) | civilware backends; app **co-tenants** its own JSON buckets in the indexer's bbolt file | Active Fyne GUI platform | Embeds `civilware/Gnomon` |
| **DHEBP/HOLOGRAM** | Consumer (TELA browser/studio) | civilware bbolt-or-gravdb index (**defaults gravdb**) **+** a separate hand-rolled graviton **content cache** | Active; v1.0.7 (2026-06-17); MIT | Embeds `civilware/Gnomon` |
| **DEROFDN/Engram** | Consumer (GUI wallet) | civilware bbolt (default) / gravdb opt-in | Active; pins Gnomon v0.0.0-20240403; v0.6.1 Beta | Embeds `civilware/Gnomon` directly |
| **civilware/tela** (`cmd/tela-cli`) | Consumer (TELA discovery CLI) | civilware bbolt (default) / gravdb opt-in via `shards.GetDBType()` | Actively maintained | Embeds `civilware/Gnomon` |
| **HyperGnomon** (ours) | Arena-accelerated indexer + TELA content layer | **bbolt-only** (pluggable factory; sqlite deferred, graviton rejected); BE8 ordered keys; typed binary; merge-on-write | Active (`feat/pluggable-storage-backends`) | Library via `pkg/gnomes/{indexer,storage,structures}` |

Practical takeaway: of the civilware direct forks, only siteraiser (SQLite) is a real storage
divergence; `moralpriest` is upstream + one cosmetic log commit and `Azylem`/`SixofClubsss`
are stale 2023 mirrors and can be dropped from further investigation
(`api.github.com/repos/civilware/Gnomon/compare/main...{moralpriest,Azylem,SixofClubsss}:main`).

---

## 2. Indexer-fork deep notes

### 2.1 civilware/Gnomon — the baseline (bbolt + graviton, why both)

Gnomon ships **two engines behind one interface with an identical logical layout** — same
bucket/tree names, same string-key scheme (`storage/bbolt.go` ~36 KB, `storage/gravdb.go` ~63 KB;
`go.mod` lists both `go.etcd.io/bbolt v1.3.6` and `github.com/deroproject/graviton`). Namespaces
(bbolt buckets == graviton trees): `stats` (`lastindexedheight` + per-txtype counts), `owner`/`scowner`,
`normaltxwithscid`, a per-SCID tree named `{scid}`, `getinfo`, `{scid}vars`, `{scid}heights`,
`invalidscids`, `miniblocks`, `blockcount`.

Key facts that shaped our design:

- **Keys are plain UTF-8 strings.** Heights are `strconv.FormatInt(h, 10)` **decimal strings**
  — not big-endian, not zero-padded (`storage/bbolt.go` `StoreSCIDVariableDetails`). Invoke keys
  are composite colon-delimited: `signer:txidPrefix:topoheight:entrypoint`. Values are
  `json.Marshal` blobs.
- **Writes are append-only JSON** — read array → unmarshal → linear dup-check → append →
  remarshal → `Put` (`StoreNormalTxWithSCIDByAddr`, `StoreSCIDInteractionHeight`,
  `StoreInvalidSCIDDeploys`), plus a trivial read-increment-write counter
  (`StoreMiniblockCountByAddress`). **No min/max/sum merge, no dual-write class index.**
- **Graviton cannot do ordered range scans.** `graviton/cursor.go` documents the cursor as
  traversing "all key/value pairs in a tree in **hash sorted order**" and exposes only
  `First/Last/Next/Prev` — **no `Seek`**
  (`raw.githubusercontent.com/deroproject/graviton/master/cursor.go`, verified). `tree.go`
  exposes only `Get/Put/Delete/Hash/GenerateProof`.
- **So every non-exact query is a full scan + client-side sort.** `gravdb.go`
  `GetAllSCIDInvokeDetails` reads all entries then `sort.SliceStable(...Height...)`. Even the
  bbolt backend iterates `Cursor.First()/Next()` with no `Seek`, leaning on lexicographic order.
- No schema-versioning/migration framework; resume is a single `lastindexedheight` string in
  `stats`. There is a one-time graviton→bbolt dump (`StoreAltDBInput`) and a RAM→disk sync.

Why ship both? Graviton gives immutable snapshot-per-commit versioning with rollback to
`GetVersion()-1` on a nil-tree/corruption (`gravdb.go`), and a `NewMemStore` RAM mode. bbolt gives
a simple single-writer transactional file. Crucially, **neither backend serves the ordered
prefix/range windows HyperGnomon needs** — that capability simply does not exist upstream.

### 2.2 secretnamebasis/simple-gnomon — what "simplified" drops

The bbolt parent that siteraiser later SQLite-ified. `go.mod` declares `go.etcd.io/bbolt v1.3.7`
direct, graviton only as a transitive derohe dep — **no sqlite, no graviton in the db layer**
(`raw.githubusercontent.com/secretnamebasis/simple-gnomon/main/go.mod`). The whole storage layer
is a **single hand-rolled `db/bbolt.go`** (~41 KB) with **no backend interface/seam**
(`api.github.com/repos/secretnamebasis/simple-gnomon/contents/db` returns only `bbolt.go`).

What it drops vs civilware: graviton entirely; the pluggable abstraction; cross-block batching.
What it keeps (and what matters to us): **read-modify-merge-on-write of JSON arrays** for
interaction-heights, normal-tx-by-addr, and invalid-scid maps (`StoreSCIDInteractionHeight`,
`StoreNormalTxWithSCIDByAddr`, `StoreInvalidSCIDDeploys` all do read→unmarshal→append→remarshal→`Put`).
Each logical write is its own `db.Update`, serialized by a `bbs.Writing` boolean with a 10 ms retry
sleep — **not** N-blocks-per-atomic-batch. Heights are unpadded decimal strings; variables-at-height
is served by scanning the whole `{scid}vars` bucket, `sort.SliceStable` on heights, then accumulating
(`GetSCIDVariableDetailsAtTopoheight`) — O(N) per query, the exact failure mode our BE8 ordered keys
were designed to avoid.

### 2.3 siteraiser/simple-gnomon — the SQLite case study

This is the one shipped SQL gnomon, and the only real counter-evidence to our defer-sqlite call.
Verified directly against `db/sqlite.go` and `go.mod`
(`raw.githubusercontent.com/siteraiser/simple-gnomon/main/...`).

**Driver & module:** SQLite via **cgo** `github.com/mattn/go-sqlite3 v1.14.33`, module `gnomon`,
go 1.25.3; graviton present only as an indirect derohe dep. (Contrast: HyperGnomon would use the
pure-Go **`modernc.org/sqlite`** if it ever un-defers — no cgo toolchain, cross-compiles cleanly.)

**Schema — 6 tables:**

```sql
state        (name TEXT, value INTEGER)
settings     (name TEXT PRIMARY KEY, value TEXT)
scs          (scs_id INTEGER PRIMARY KEY, scid TEXT UNIQUE NOT NULL, owner TEXT NOT NULL,
              height INTEGER, scname TEXT, scdescr TEXT, scimgurl TEXT, class TEXT, tags TEXT)
variables    (v_id INTEGER PRIMARY KEY, height INTEGER, txid TEXT, vars TEXT)
invokes      (scid TEXT, signer TEXT, txid TEXT UNIQUE, height INTEGER, entrypoint TEXT)
interactions (height INTEGER, txid TEXT UNIQUE, scid TEXT)
```

**Only two indexes:** `CREATE INDEX height_index ON interactions(scid,txid)` and
`CREATE INDEX invokes_height_index ON invokes(txid)`.

**Write model — append-only, no merge.** `StoreSCIDVariableDetails` is a pure
`INSERT INTO variables (height, txid, vars) VALUES (?,?,?)` — one immutable row per invocation,
the full vars map stored as opaque **TEXT** (JSON). `scs` relies on the `scid UNIQUE` constraint;
`settings` uses `REPLACE INTO`. **No min/max/sum merge-on-write, no dual-write class index.**

**Prefix/range "scan" in SQL.** There is no Seek; the relational analog is the secondary index
plus a range predicate. `GetSCIDVariableDetailsAtTopoheight` runs
`... WHERE height <= ? ORDER BY height ASC` and **folds history to latest-value-per-key in Go**
(`getTypedVariables`, a `vs2k` map). This is the same accumulate-on-read shape as the bbolt parent —
read cost grows with SC history and there is **no topoheight-window pruning** (it pulls everything
≤ target rather than a `[lo, hi]` window). HyperGnomon's BE8 height-prefixed keys + `Seek` give a
true O(window) range; the SQL index here is closer to O(history-up-to-height).

**Un-tuned, but genuinely RAM-first.** No `PRAGMA` tuning at all — no WAL, no `synchronous`, no
`journal_mode`; statements are `Prepare()`'d **per call** (not cached); writes are **per-row
autocommits**, not a batched transaction; concurrency is a global mutex (`ready(false)`/`ready(true)`),
not SQLite transactions. But it *does* run **memory-first** (verified in `NewSqlDB`): it opens
`sql.Open("sqlite3", "file:diskdb?mode=memory&cache=shared")`, `ATTACH`es the on-disk file, loads at
startup via `INSERT…SELECT`, and persists with SQLite's native **`Backup()` API** (`bk.Step(-1)`) plus
a selective `WriteToDisk` (`INSERT INTO diskdb.* SELECT … WHERE height >= …`). So the whole index lives
in RAM with periodic disk snapshots — clever at simple-wallet scale, but **memory ≈ full-index size**,
which is exactly why it would not scale to a multi-GB full-chain index the way bbolt's mmap does. The
canonical SQLite write-amp fix (WAL + `synchronous=NORMAL` + one batched txn) that HyperGnomon gets
*structurally* from its bbolt batch (N blocks → 1 atomic txn) is simply absent here.

**The crux for our decision:** siteraiser **avoids** per-row merge-on-write by going
append-only + aggregate-on-read. So it never pays the merge cost our argument said would erase
sqlite's bulk-insert advantage — **but it also never reproduces HyperGnomon's semantics**
(min/max/sum counts, dual-write class index, fan-one-batch-across-~17-buckets). It is therefore
not a like-for-like proof either way (see §5).

### 2.4 Other forks' divergence

`moralpriest/Gnomon` differs from upstream by exactly one non-storage commit (a 1-line move of a
`"Waiting on GetInfo..."` log in `indexer/indexer.go`, +1/-1;
`api.github.com/repos/moralpriest/Gnomon/commits/main`); its `go.mod` is byte-identical to upstream
in storage terms. `Azylem/Gnomon` and `SixofClubsss/Gnomon` are stale mirrors terminating at
civilware's Nov-2023 `v2.0.0-alpha.1` line. None attempt SQL, badger, or leveldb; all carry
`bbolt + graviton` verbatim. Net: no ecosystem demand-pull toward SQL from the fork set.

---

## 3. Consumer integration patterns

### 3.1 dReams — the `StartGnomon` wrapper (the shape we must stay compatible with)

`dReam-dApps/dReams/gnomes/` is the canonical in-process embed that most DERO dApps reuse.
Verified signature (`raw.githubusercontent.com/dReam-dApps/dReams/main/gnomes/gnomon.go`):

```go
func StartGnomon(tag, dbtype string, filters []string, upper, lower int, custom func())
```

It constructs the chosen backend (`NewBoltDB` when `dbtype == "boltdb"`, else `NewGravDB`, the
other returned `nil`) and passes **both handles positionally** into the upstream constructor:

```go
gnomes.Indexer = indexer.NewIndexer(grav_backend, bolt_backend, dbtype,
    filters, last_height, rpc.Daemon.Rpc, "daemon", false, false, &gnomes.Fast, exclusions)
```

— then `go gnomes.Indexer.StartDaemonMode(...)`. Import is `github.com/civilware/Gnomon/indexer`
(not HyperGnomon). dReams itself passes `"boltdb"`. The extension points are a parameterless
`custom func()` fired once fastsync reaches the lower threshold and status `== "indexed"`, plus
`AddSCIDToIndex(...)` to inject specific SCIDs at runtime; the consumer polls a `Writing()/IsWriting()`
write-barrier before doing its own bbolt writes.

**Co-tenant anti-pattern to note (not adopt):** dReams stores app-level state *inside Gnomon's own
bbolt file* via generic `StoreBolt/GetStorage/DeleteStorage/StorageExists` helpers operating on
`gnomes.Indexer.BBSBackend.DB` (`gnomes/boltdb.go`), JSON-marshaled under caller-supplied
`(bucket, key)`. Convenient for a single-file GUI app, but it lets external callers write arbitrary
buckets into the indexer's transactional store — defeating typed-binary encoding and schema
discipline. HyperGnomon should keep storage encapsulated and return data for the consumer to store
separately. The signal to keep: **consumers want one embedded file and a stable wrapper API.**

### 3.2 HOLOGRAM — graviton as a *versioned content cache* (the legit graviton use)

`DHEBP/HOLOGRAM` is the canonical in-the-wild graviton consumer, and it shows precisely what
graviton **is** good for — and what it is **not** asked to do. Two independent uses:

1. **Gnomon index** (civilware), pluggable bbolt-or-gravdb, **defaulting to `gravdb`** — but
   treated as a *disposable, rebuildable cache*: filter changes (SHA-256 `currentFilterVersion`)
   trigger auto-resync and corruption auto-recovers by rebuild. It is safe on hash-order storage
   **only because it never needs ordered scans** — all reads are point/per-SC
   (`GetAllSCIDVariableDetails`, `GetSCIDValuesByKey`). (`gnomon.go`)

2. A **separate hand-rolled graviton content cache** (`offline_cache.go`, verified): four trees
   `cached_apps` / `cached_content` / `cache_manifest` / `cache_stats`. Content retrieval is a
   strict **point-get by composite key** `key := fmt.Sprintf("%s:%s", scid, path)` →
   `contentTree.Get([]byte(key))` over `LoadSnapshot(0)`. Multi-tree writes are atomic via
   `graviton.Commit(contentTree, manifestTree, appsTree)`. **`Cursor` is used only in
   `GetCachedApps()`/`getStats()` to enumerate the handful of cached apps** — never to range-scan
   content by height/sequence. "Versioning" is faked via explicit metadata fields
   (`OnChainVersion`, `HasUpdate`, `version > cachedApp.Version`), not by walking graviton's
   snapshot history.

So the genuine graviton use case is **versioned point-get blob/metadata caching**, which is
orthogonal to ordered indexing. When HOLOGRAM needs to *discover/index* SCs, it reaches for Gnomon —
not graviton.

### 3.3 Engram / dApp suite / TELA-CLI — embedding norms

`DEROFDN/Engram` (GUI wallet) embeds Gnomon directly (`go.mod` pins
`civilware/Gnomon v0.0.0-20240403...`, no dReams). `civilware/tela` `cmd/tela-cli` embeds Gnomon to
scan TELA SCs, reading `DBType` from `shards.GetDBType()` and opening
`storage.NewBBoltDB(path,"gnomon")` / `storage.NewGravDB(path,"25ms")`. The SixofClubsss game suite
(Holdero, Baccarat, dPrediction, Duels) consumes Gnomon **through** dReams' `gnomes` package rather
than re-implementing the embed. Across all of them: **bbolt is the default; gravdb is a
rarely-used opt-in; nobody uses SQL; and the storage surface they actually need is KV**
(`GetSCIDValuesByKey`, `GetAllOwnersAndSCIDs`), not relational queries.

---

## 4. Tricks worth stealing for HyperGnomon

| # | Trick (source) | Tag | Why (one line) |
|---|---|---|---|
| 1 | `bbolt.Compact()` with 64 MB `txMaxSize` for periodic backup/compaction (secretnamebasis db/bbolt.go) | **steal ✓ measured** | **VALIDATED:** `bbolt.Compact()` reclaimed **93.8%** (64 MiB → 4 MiB) on a fragmented DB. Cheap, low-risk; adopt scheduled compaction. (`storage/dbbench/trick_validation_test.go`) |
| 2 | Append-only variable history + **aggregate-on-read** as an alternative to merge-on-write (siteraiser `StoreSCIDVariableDetails` / `getTypedVariables`) | **REJECTED ✗ measured** | **BENCHMARKED & FAILED:** on a merge-style index (AddrSCID), append-only is **7.3× slower on writes** (76 µs → 558 µs/batch; unbounded tree growth on repeated keys) **and 1.6–24× slower on reads** (flat ~530 ns vs O(depth): 13.5 µs @ depth 1000). Merge-on-write wins decisively. The "measure before believing" caveat held. (`storage/dbbench/trick_validation_test.go`) |
| 3 | Runtime backend selection via a `dbtype` string threaded to the constructor (dReams `StartGnomon`; tela-cli) | **already-have** | Validates our `storage/factory.go`; our clean Store interface beats their nil-one-backend positional args. |
| 4 | Centralize embedding in ONE wrapper package every consumer imports (dReams `gnomes`) | **already-have** | Exactly our `pkg/gnomes` library-mode positioning; the migration target is the wrapper layer. |
| 5 | Multi-tree atomic commit to keep content+manifest+metadata consistent (HOLOGRAM `graviton.Commit(...)`) | **already-have** | Same idea as our one-`FlushBatch`-fans-across-~17-buckets in a single bbolt `Update` txn — our engine. |
| 6 | SQLite WAL + `synchronous=NORMAL` + batched single-txn bulk insert (notably **absent** in siteraiser) | **already-have** | We get the equivalent property structurally via bbolt batch; siteraiser's omission means its SQLite is *slower than tuned*, weakening "sqlite bulk-insert win." |
| 7 | Composite covering index `interactions(scid,txid)` (siteraiser) | **already-have** | Relational analog of our BE8 height-prefixed ordered keys; no new idea. |
| 8 | Composite colon-delimited string keys `signer:txid:height:entrypoint` (civilware/siteraiser) | **already-have / skip** | Human-readable but lexicographic & unpadded — strictly worse than BE8 binary; confirms our choice. |
| 9 | graviton immutable snapshot-per-commit + rollback to `GetVersion()-1` on corruption (civilware `gravdb.go`) | **skip** | Nice crash-recovery, but bundled with hash-order/no-Seek — the exact thing that kills Route-B scans. Not worth importing the engine. |
| 10 | Client-side `sort.SliceStable` by height after a full scan to fake ordered queries (civilware/secretnamebasis) | **skip** | The O(N) anti-pattern our BE8 keys + Seek windowing exist to avoid; do not regress. |
| 11 | Append-only JSON arrays per key, read-unmarshal-append-remarshal (civilware/secretnamebasis) | **skip** | O(n) rewrite per append + JSON overhead on the hot path; our typed-binary + merge-on-write is the deliberate upgrade. |
| 12 | Per-SCID dynamic buckets (`{scid}vars`, `{scid}heights`) (secretnamebasis) | **skip** | Unbounded bucket count, prevents global ordered scans; our fixed ~17-bucket model is better at scale. |
| 13 | Read-time TEXT→typed coercion of vars (siteraiser `getTyped`) | **skip** | Stores opaque TEXT then re-parses per query — opposite of our typed-binary hot path; a regression for our perf goals. |
| 14 | App co-tenanting its JSON buckets in the indexer's bbolt handle (dReams `boltdb.go`) | **skip** | Exposing the raw DB handle breaks encapsulation/typed encoding; consumers *want* a co-tenant KV, but serve it via library API instead. |

---

## 5. Reality-check on our decisions

We set out to be intellectually honest, especially because a SQLite gnomon really did ship.

### 5.1 Defer SQLite — **stands, with a sharpened justification**

What we got right:
- The named counter-example (`siteraiser/simple-gnomon`) is real but **does not test our actual
  claim**. Our argument was "a SQL backend's per-row merges would erase sqlite's bulk-insert win on
  *our* merge-heavy schema." siteraiser **avoids merges entirely** — append-only INSERT +
  aggregate-on-read — so it never reproduces our min/max/sum counters, dual-write class index, or
  one-batch-across-~17-buckets. It proves SQLite is viable for a *simple overwrite/append* gnomon,
  not for HyperGnomon's workload.
- Its SQLite is also **un-tuned**: no WAL/PRAGMA, per-call `Prepare`, per-row commits, no batched
  txn, only two indexes, `WHERE height <= ?` with no `[lo,hi]` window pruning. So even its
  bulk-insert path is unoptimized — it is **weak evidence for "sqlite is faster,"** not strong.
- The lighter parents (secretnamebasis, civilware) likewise do not do per-row merge, so the broader
  ecosystem doesn't demonstrate a SQL win on our schema either. And "not a SQL engine" remains a
  stated non-goal — unchallenged by any fork.

The nuance we must record (where our original framing over-claimed):
- Our **strongest** justification for defer was sometimes phrased as "we already beat SQL because of
  ordered range scans + merge-on-write." That advantage is **HyperGnomon-proprietary**, not
  inherited from the baseline — civilware/secretnamebasis use unpadded decimal-string height keys
  with client-side sort, *not* BE8 Seek windows. So the honest statement is: **defer-sqlite is
  defensible, but it must be defended by benchmarking HyperGnomon's BE8 + merge-on-write
  FlushBatch against a *tuned* sqlite (WAL + batched txn + window-pruned range), not asserted from
  siteraiser**, whose design is both different (append-only) and slower (un-tuned). If we ever
  un-defer, the obvious idea to import was siteraiser's append-only-history + read-side fold as an
  *alternative* to merge-on-write. **We benchmarked it (§4 #2) and it FAILED** — 7.3× slower writes
  (unbounded tree growth on repeated keys) and up to 24× slower reads (cost scales with history).
  Merge-on-write stays. So even the one importable idea from the SQL gnomon does not survive
  measurement on our schema.

Net: **keep sqlite deferred.** The one shipped SQL gnomon either skips our hard semantics or runs
un-tuned; it is not a reason to un-defer, but it *is* a reason to make the defer rest on a real
benchmark rather than an inherited assumption.

### 5.2 Reject graviton — **confirmed, strongly**

What we got right, now backed by upstream source rather than assumption:
- `graviton/cursor.go` documents iteration in **"hash sorted order"** and exposes only
  `First/Last/Next/Prev` with **no `Seek`** (verified). `tree.go` exposes only
  `Get/Put/Delete/Hash/GenerateProof`. Graviton **cannot** serve prefix/ordered-range windows at the
  engine level — reason (2) is now proven from the engine's own code.
- The canonical Gnomon is therefore *forced* into full-scan-then-`sort.SliceStable` for every range
  query (`gravdb.go`) — exactly the O(N)-vs-O(window) failure we cited.
- Real-world consumers confirm the boundary: **HOLOGRAM uses graviton only as a point-get
  versioned content cache** (`offline_cache.go`, composite-key `scid:path`, cursor reserved for
  enumerating a tiny app list), and when it needs to *index/discover* it reaches for Gnomon, not
  graviton. dReams/Engram/tela-cli all **default to bbolt** and treat gravdb as a rarely-used opt-in.
- No graviton-only fork exists in the tree; nobody extended graviton with Seek/ordered-range.

Net: **keep graviton rejected.** Cite HOLOGRAM `offline_cache.go` as the *positive* example of what
graviton is for (versioned point-get cache), contrasted with the Route-B BE8 ordered scans it cannot do.

### 5.3 One caution to log

The existence of a working gravdb Gnomon backend (HOLOGRAM defaults to it) shows a consumer happily
running Gnomon on hash-order storage — but **only because that consumer never needs ordered range
scans**. That is precisely the capability that differentiates HyperGnomon, and the reason the
graviton-reject is correct *for us* even though graviton is fine *for them*.

---

## 6. Sources

- civilware/Gnomon — `github.com/civilware/Gnomon/tree/main/storage` (`bbolt.go`, `gravdb.go`),
  `.../main/indexer/indexer.go`, `go.mod`
- graviton engine — `raw.githubusercontent.com/deroproject/graviton/master/cursor.go` (verified: "hash sorted order", no `Seek`), `tree.go`
- secretnamebasis/simple-gnomon — `raw.githubusercontent.com/secretnamebasis/simple-gnomon/main/go.mod`,
  `db/bbolt.go`, `api.github.com/repos/secretnamebasis/simple-gnomon/contents/db`
- siteraiser/simple-gnomon — `raw.githubusercontent.com/siteraiser/simple-gnomon/main/db/sqlite.go` (verified: 6 tables, 2 indexes, no PRAGMA, append-only INSERT, `WHERE height <= ? ORDER BY height ASC`, per-row commits, per-call Prepare, `:memory:`+ATTACH+`Backup()` RAM-first), `.../main/go.mod` (verified: `mattn/go-sqlite3 v1.14.33`, module `gnomon`, go 1.25.3)
- fork comparisons — `api.github.com/repos/civilware/Gnomon/compare/main...{moralpriest,Azylem,SixofClubsss}:main`; `api.github.com/repos/moralpriest/Gnomon/commits/main`; `api.github.com/repos/secretnamebasis/simple-gnomon/forks`
- dReams — `raw.githubusercontent.com/dReam-dApps/dReams/main/gnomes/gnomon.go` (verified `StartGnomon` signature + `NewIndexer` call + civilware import), `gnomes/gnomes.go`, `gnomes/boltdb.go`
- HOLOGRAM — `raw.githubusercontent.com/DHEBP/HOLOGRAM/dev/gnomon.go`, `.../dev/offline_cache.go` (verified four trees, `scid:path` point-get, `graviton.Commit`, cursor only for app enumeration), `.../dev/README.md`
- Engram — `github.com/DEROFDN/Engram/go.mod`
- tela-cli — `github.com/civilware/tela/cmd/tela-cli/gnomon.go`; `tela.derod.org/tela-cli/gnomon-guide`
- Gnomon storage characterization — `derod.org/tools/gnomon` (BoltDB + GravitonDB)
