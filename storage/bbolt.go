package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

var logger = logrus.WithField("pkg", "storage")

// Bucket names
var (
	bucketStats   = []byte("stats")
	bucketOwners  = []byte("owners")
	bucketHeaders = []byte("headers")
	bucketClass   = []byte("class") // Route B: "<class>|<BE8:h>|<scid>" -> ClassMeta msgpack
	bucketTags    = []byte("tags")  // reserved; populated when tag indexes ship
	bucketHeight  = []byte("height")
	bucketScVars  = []byte("scvars")
	bucketNormTx  = []byte("normaltxwithscid")
	bucketInvalid = []byte("invalidscidinvokes")

	// Route B (DESIGN.md §3) buckets.
	bucketBlockHash   = []byte("blockhashes")  // BE8 height -> 64-hex block hash
	bucketInstalls    = []byte("installs")     // "<BE8:h>|<scid>" -> InstallRecord msgpack
	bucketClassIdx    = []byte("class_scid")   // scid -> ClassMeta msgpack (O(1) lookup)
	bucketAddrSCIDs   = []byte("addr_scids")   // parent; per-addr sub-bucket created on demand
	bucketTELAContent = []byte("tela_content") // scid|path -> {body, mime, sha256}
)

// Key layout helpers: keep binary big-endian so bbolt's byte-order cursor
// walk matches numeric order. Height 0..2^63-1 packs into 8 bytes.

// encHeight BE-encodes a uint64 height into an 8-byte slice.
func encHeight(h int64) []byte {
	var b [8]byte
	u := uint64(h)
	b[0] = byte(u >> 56)
	b[1] = byte(u >> 48)
	b[2] = byte(u >> 40)
	b[3] = byte(u >> 32)
	b[4] = byte(u >> 24)
	b[5] = byte(u >> 16)
	b[6] = byte(u >> 8)
	b[7] = byte(u)
	return b[:]
}

// decHeight reverses encHeight.
func decHeight(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]))
}

// installKey packs <BE8:h>|<scid> for prefix scans by height.
func installKey(h int64, scid string) []byte {
	k := make([]byte, 0, 8+1+len(scid))
	k = append(k, encHeight(h)...)
	k = append(k, '|')
	k = append(k, scid...)
	return k
}

// classKey packs <class>|<BE8:h>|<scid> for prefix scans by class.
func classKey(class string, h int64, scid string) []byte {
	k := make([]byte, 0, len(class)+1+8+1+len(scid))
	k = append(k, class...)
	k = append(k, '|')
	k = append(k, encHeight(h)...)
	k = append(k, '|')
	k = append(k, scid...)
	return k
}

// BboltStore implements Storage backed by BoltDB.
//
// No external mutex: bbolt is already single-writer internally (db.rwlock in
// bolt.Tx.WriteBatch path serializes Update calls). Wrapping Update in another
// Lock/Unlock is pure overhead.
type BboltStore struct {
	DB   *bolt.DB
	Path string
}

// NewBboltStore opens or creates a BoltDB database.
func NewBboltStore(dbDir string, searchFilter string) (*BboltStore, error) {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("create db dir %s: %w", dbDir, err)
	}
	dbPath := filepath.Join(dbDir, "HYPERGNOMON.db")

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout:      0,    // 0 = wait indefinitely for lock
		NoGrowSync:   true, // skip fsync on db file growth — safe because NoSync already skips fsync
		NoSync:       true, // skip fsync during initial sync for speed; call EnableSync() at chain tip
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, fmt.Errorf("bbolt open %s: %w", dbPath, err)
	}

	store := &BboltStore{DB: db, Path: dbPath}

	// Create all buckets
	err = db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{
			bucketStats, bucketOwners, bucketHeaders,
			bucketClass, bucketTags, bucketHeight,
			bucketScVars, bucketNormTx, bucketInvalid,
			// Route B M0
			bucketBlockHash, bucketInstalls, bucketClassIdx,
			bucketAddrSCIDs, bucketTELAContent,
		}
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("bbolt init buckets: %w", err)
	}

	logger.Infof("BoltDB opened: %s", dbPath)
	return store, nil
}

// EnableSync re-enables fsync after initial sync is complete (caught up to chain tip).
func (s *BboltStore) EnableSync() {
	s.DB.NoSync = false
	if err := s.DB.Sync(); err != nil {
		logger.Warnf("EnableSync: bbolt Sync returned: %v", err)
	}
	logger.Info("BoltDB sync enabled (caught up to chain tip)")
}

// DisableSync disables fsync for bulk initial sync performance.
func (s *BboltStore) DisableSync() {
	s.DB.NoSync = true
	logger.Info("BoltDB sync disabled (initial sync mode)")
}

func (s *BboltStore) Close() error {
	return s.DB.Close()
}

func (s *BboltStore) GetLastIndexHeight() (int64, error) {
	var height int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStats)
		v := b.Get([]byte("lastindexedheight"))
		if v == nil {
			return nil
		}
		var err error
		height, err = strconv.ParseInt(string(v), 10, 64)
		return err
	})
	return height, err
}

func (s *BboltStore) StoreLastIndexHeight(height int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketStats).Put(
			[]byte("lastindexedheight"),
			[]byte(strconv.FormatInt(height, 10)),
		)
	})
}

func (s *BboltStore) StoreOwner(scid, owner string) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOwners).Put([]byte(scid), []byte(owner))
	})
}

func (s *BboltStore) GetOwner(scid string) (string, error) {
	var owner string
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketOwners).Get([]byte(scid))
		if v != nil {
			owner = string(v)
		}
		return nil
	})
	return owner, err
}

func (s *BboltStore) StoreInvokeDetails(scid, sender, entrypoint string, height int64, details *structures.SCTXParse) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(scid))
		if err != nil {
			return err
		}
		// Full Txid: 8-char prefix only gives 32 bits of entropy and collides at scale.
		key := fmt.Sprintf("%s:%s:%d:%s", sender, details.Txid, height, entrypoint)
		val, err := msgpack.Marshal(details)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
}

func (s *BboltStore) GetInvokeDetailsBySCID(scid string) ([]*structures.SCTXParse, error) {
	var results []*structures.SCTXParse
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(scid))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var detail structures.SCTXParse
			if err := msgpack.Unmarshal(v, &detail); err != nil {
				return err
			}
			results = append(results, &detail)
			return nil
		})
	})
	return results, err
}

func (s *BboltStore) StoreSCIDVariableDetails(scid string, vars []*structures.SCIDVariable, height int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketScVars)
		key := fmt.Sprintf("%s:%d", scid, height)
		val, err := msgpack.Marshal(vars)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
}

func (s *BboltStore) GetSCIDVariableDetailsAtHeight(scid string, height int64) ([]*structures.SCIDVariable, error) {
	var vars []*structures.SCIDVariable
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketScVars)
		key := fmt.Sprintf("%s:%d", scid, height)
		v := b.Get([]byte(key))
		if v == nil {
			return nil
		}
		// Tag-byte dispatch: 0x02 is typed v1, 0x9X is legacy msgpack array.
		if structures.IsSCIDVariablesTyped(v) {
			parsed, err := structures.UnmarshalSCIDVariablesTyped(v)
			if err != nil {
				return err
			}
			vars = parsed
			return nil
		}
		return msgpack.Unmarshal(v, &vars)
	})
	return vars, err
}

func (s *BboltStore) StoreSCIDInteractionHeight(scid string, height int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketHeight)
		existing := b.Get([]byte(scid))
		var heights []int64
		if existing != nil {
			if err := msgpack.Unmarshal(existing, &heights); err != nil {
				logger.Warnf("heights decode for %s: %v (starting fresh list)", scid, err)
				heights = nil
			}
		}
		heights = append(heights, height)
		val, err := msgpack.Marshal(heights)
		if err != nil {
			return err
		}
		return b.Put([]byte(scid), val)
	})
}

func (s *BboltStore) GetSCIDInteractionHeights(scid string) ([]int64, error) {
	var heights []int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketHeight).Get([]byte(scid))
		if v == nil {
			return nil
		}
		return msgpack.Unmarshal(v, &heights)
	})
	return heights, err
}

func (s *BboltStore) StoreNormalTxWithSCIDByAddr(addr string, ntx *structures.NormalTXWithSCIDParse) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNormTx)
		existing := b.Get([]byte(addr))
		var txs []*structures.NormalTXWithSCIDParse
		if existing != nil {
			if err := msgpack.Unmarshal(existing, &txs); err != nil {
				logger.Warnf("normaltx decode for %s: %v (starting fresh list)", addr, err)
				txs = nil
			}
		}
		txs = append(txs, ntx)
		val, err := msgpack.Marshal(txs)
		if err != nil {
			return err
		}
		return b.Put([]byte(addr), val)
	})
}

func (s *BboltStore) StoreInvalidSCIDDeploys(scid string, fees uint64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketInvalid).Put([]byte(scid), []byte(strconv.FormatUint(fees, 10)))
	})
}

func (s *BboltStore) StoreTxCounts(reg, burn, norm int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStats)
		// Atomic increment: read existing + add new
		addCount := func(key string, delta int64) error {
			existing := b.Get([]byte(key))
			var current int64
			if existing != nil {
				current, _ = strconv.ParseInt(string(existing), 10, 64)
			}
			return b.Put([]byte(key), []byte(strconv.FormatInt(current+delta, 10)))
		}
		if err := addCount("regtxcount", reg); err != nil {
			return err
		}
		if err := addCount("burntxcount", burn); err != nil {
			return err
		}
		return addCount("normtxcount", norm)
	})
}

func (s *BboltStore) GetAllSCIDs() ([]string, error) {
	var scids []string
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOwners).ForEach(func(k, v []byte) error {
			scids = append(scids, string(k))
			return nil
		})
	})
	return scids, err
}

func (s *BboltStore) GetAllOwnersAndSCIDs() (map[string]string, error) {
	result := make(map[string]string)
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOwners).ForEach(func(k, v []byte) error {
			result[string(k)] = string(v)
			return nil
		})
	})
	return result, err
}

// FlushBatch atomically writes all accumulated data in a single BoltDB transaction.
// This is the arena-pattern payoff: one lock acquisition for potentially thousands of records.
func (s *BboltStore) FlushBatch(batch *WriteBatch) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		// Store owners
		ownerBucket := tx.Bucket(bucketOwners)
		for scid, owner := range batch.Owners {
			if err := ownerBucket.Put([]byte(scid), []byte(owner)); err != nil {
				return fmt.Errorf("batch owner %s: %w", scid, err)
			}
		}

		// keyBuf is reused across both the invocation and variable loops below.
		// bbolt.Bucket.Put copies the key internally (see bolt.Bucket.Put → node.put
		// which copies into its own arena), so reusing a single backing slice is safe.
		// Pre-size for the worst common case: 64-char addr + ':' + 64-char txid +
		// ':' + up to 20-char int + ':' + ~32-char entrypoint ≈ 184 bytes.
		keyBuf := make([]byte, 0, 192)

		// Store invocations.
		// Old path used fmt.Sprintf("%s:%s:%d:%s", ...) which allocates a fresh string
		// per invocation. Replaced with append + strconv.AppendInt to produce the
		// exact same key bytes without any per-iteration allocation.
		for _, inv := range batch.Invocations {
			b, err := tx.CreateBucketIfNotExists([]byte(inv.Scid))
			if err != nil {
				return fmt.Errorf("batch invoke bucket %s: %w", inv.Scid, err)
			}
			keyBuf = keyBuf[:0]
			keyBuf = append(keyBuf, inv.Sender...)
			keyBuf = append(keyBuf, ':')
			keyBuf = append(keyBuf, inv.Details.Txid...)
			keyBuf = append(keyBuf, ':')
			keyBuf = strconv.AppendInt(keyBuf, inv.Height, 10)
			keyBuf = append(keyBuf, ':')
			keyBuf = append(keyBuf, inv.Entrypoint...)
			val, err := msgpack.Marshal(inv.Details)
			if err != nil {
				return fmt.Errorf("batch invoke marshal: %w", err)
			}
			if err := b.Put(keyBuf, val); err != nil {
				return err
			}
		}

		// Store variables.
		// Same treatment: fmt.Sprintf("%s:%d", scid, height) → append + AppendInt.
		// Value is now v1 typed (C7 optimization): 6→0 marshal allocs per snapshot.
		// Reader (GetSCIDVariableDetailsAtHeight) dispatches on byte[0] so legacy
		// msgpack-encoded values remain readable until overwritten.
		varBucket := tx.Bucket(bucketScVars)
		// Reusable value buffer across every Put — bbolt copies on Put.
		varValBuf := make([]byte, 0, 256)
		for scid, heightVars := range batch.Variables {
			for height, vars := range heightVars {
				keyBuf = keyBuf[:0]
				keyBuf = append(keyBuf, scid...)
				keyBuf = append(keyBuf, ':')
				keyBuf = strconv.AppendInt(keyBuf, height, 10)
				varValBuf = varValBuf[:0]
				varValBuf = structures.MarshalSCIDVariablesTypedAppend(varValBuf, vars)
				if err := varBucket.Put(keyBuf, varValBuf); err != nil {
					return err
				}
			}
		}

		// Store interaction heights
		heightBucket := tx.Bucket(bucketHeight)
		for scid, heights := range batch.Heights {
			existing := heightBucket.Get([]byte(scid))
			var current []int64
			if existing != nil {
				if err := msgpack.Unmarshal(existing, &current); err != nil {
					logger.Warnf("batch heights decode for %s: %v (starting fresh list)", scid, err)
					current = nil
				}
			}
			current = append(current, heights...)
			val, err := msgpack.Marshal(current)
			if err != nil {
				return fmt.Errorf("batch heights marshal: %w", err)
			}
			if err := heightBucket.Put([]byte(scid), val); err != nil {
				return err
			}
		}

		// Store normal txs
		normBucket := tx.Bucket(bucketNormTx)
		for addr, txs := range batch.NormalTxs {
			existing := normBucket.Get([]byte(addr))
			var current []*structures.NormalTXWithSCIDParse
			if existing != nil {
				if err := msgpack.Unmarshal(existing, &current); err != nil {
					logger.Warnf("batch normaltx decode for %s: %v (starting fresh list)", addr, err)
					current = nil
				}
			}
			current = append(current, txs...)
			val, err := msgpack.Marshal(current)
			if err != nil {
				return err
			}
			if err := normBucket.Put([]byte(addr), val); err != nil {
				return err
			}
		}

		// Store invalid SCIDs
		invalidBucket := tx.Bucket(bucketInvalid)
		for scid, fees := range batch.InvalidSCIDs {
			if err := invalidBucket.Put([]byte(scid), []byte(strconv.FormatUint(fees, 10))); err != nil {
				return err
			}
		}

		// Store TX counts (atomic increment)
		statsBucket := tx.Bucket(bucketStats)
		addCount := func(key string, delta int64) error {
			if delta == 0 {
				return nil
			}
			existing := statsBucket.Get([]byte(key))
			var current int64
			if existing != nil {
				current, _ = strconv.ParseInt(string(existing), 10, 64)
			}
			return statsBucket.Put([]byte(key), []byte(strconv.FormatInt(current+delta, 10)))
		}
		if err := addCount("regtxcount", batch.RegTxCount); err != nil {
			return err
		}
		if err := addCount("burntxcount", batch.BurnTxCount); err != nil {
			return err
		}
		if err := addCount("normtxcount", batch.NormTxCount); err != nil {
			return err
		}

		// === Route B (DESIGN.md §3) — new buckets, same atomic txn ===

		// Block hashes for reorg detection.
		if len(batch.BlockHashes) > 0 {
			bhBucket := tx.Bucket(bucketBlockHash)
			for h, hash := range batch.BlockHashes {
				if err := bhBucket.Put(encHeight(h), []byte(hash)); err != nil {
					return fmt.Errorf("batch blockhash h=%d: %w", h, err)
				}
			}
		}

		// Installs: height-prefixed, range-scannable.
		if len(batch.Installs) > 0 {
			insBucket := tx.Bucket(bucketInstalls)
			for mapKey, rec := range batch.Installs {
				scid, h, ok := parseSCIDHeightKey(mapKey)
				if !ok {
					continue
				}
				val, err := msgpack.Marshal(rec)
				if err != nil {
					return fmt.Errorf("batch install marshal: %w", err)
				}
				if err := insBucket.Put(installKey(h, scid), val); err != nil {
					return err
				}
			}
		}

		// Class metadata: two writes per SCID.
		//   1. classIdx:  scid -> ClassMeta  (O(1) lookup)
		//   2. class:     <class>|<BE8 h>|<scid> -> ClassMeta  (prefix-scan by class)
		if len(batch.Classes) > 0 {
			classIdx := tx.Bucket(bucketClassIdx)
			classPrefix := tx.Bucket(bucketClass)
			for scid, meta := range batch.Classes {
				if meta == nil || meta.Class == "" {
					continue
				}
				val, err := msgpack.Marshal(meta)
				if err != nil {
					return fmt.Errorf("batch class marshal: %w", err)
				}
				if err := classIdx.Put([]byte(scid), val); err != nil {
					return err
				}
				if err := classPrefix.Put(classKey(meta.Class, meta.InstallHeight, scid), val); err != nil {
					return err
				}
			}
		}

		// Address reverse index: nested bucket per address, scid as key.
		// Merge with any existing entry (min/max/sum).
		//
		// v1 typed encoding (DESIGN.md §3 optimization C6): 25-byte fixed
		// layout, tag byte 0x01. Legacy msgpack records (tag 0x80-0x8f
		// fixmap) are still readable — we dispatch on byte[0]. Writes are
		// always v1, so legacy blobs get upgraded on next touch.
		if len(batch.AddrSCIDs) > 0 {
			parent := tx.Bucket(bucketAddrSCIDs)
			// Reusable encode buffer — bbolt.Put copies, so one slice serves
			// every Put across the whole addr_scids block.
			valBuf := make([]byte, 0, structures.EncodedAddrSCIDEntrySize)
			for addr, scids := range batch.AddrSCIDs {
				subBucket, err := parent.CreateBucketIfNotExists([]byte(addr))
				if err != nil {
					return fmt.Errorf("batch addr bucket %s: %w", addr, err)
				}
				for scid, delta := range scids {
					existing := subBucket.Get([]byte(scid))
					merged := delta
					if existing != nil {
						var cur structures.AddrSCIDEntry
						var decErr error
						if structures.IsAddrSCIDEntryTyped(existing) {
							decErr = cur.UnmarshalTyped(existing)
						} else {
							decErr = msgpack.Unmarshal(existing, &cur)
						}
						if decErr != nil {
							logger.Warnf("addr_scids decode %s/%s: %v (overwriting)", addr, scid, decErr)
						} else {
							merged = &structures.AddrSCIDEntry{
								FirstHeight: minInt64(cur.FirstHeight, delta.FirstHeight),
								LastHeight:  maxInt64(cur.LastHeight, delta.LastHeight),
								Count:       cur.Count + delta.Count,
							}
						}
					}
					valBuf = valBuf[:0]
					valBuf = merged.MarshalTypedAppend(valBuf)
					if err := subBucket.Put([]byte(scid), valBuf); err != nil {
						return err
					}
				}
			}
		}

		// Store last indexed height (crash recovery point). Keeping decimal
		// for backward compat with existing DBs; binary encoding will come in
		// the v1→v2 migration.
		if batch.LastHeight > 0 {
			if err := statsBucket.Put(
				[]byte("lastindexedheight"),
				[]byte(strconv.FormatInt(batch.LastHeight, 10)),
			); err != nil {
				return err
			}
		}

		return nil
	})
}

// parseSCIDHeightKey inverts scidHeightKey. Returns (scid, height, ok).
func parseSCIDHeightKey(k string) (string, int64, bool) {
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
	h, err := strconv.ParseInt(k[at+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return k[:at], h, true
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ========================================================================
// Route B — storage.Storage interface additions
// ========================================================================

// StoreBlockHash writes a block hash outside a FlushBatch. Prefer
// WriteBatch.AddBlockHash for the normal path — this method exists for
// recovery/truncate tools.
func (s *BboltStore) StoreBlockHash(height int64, hash string) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBlockHash).Put(encHeight(height), []byte(hash))
	})
}

// GetBlockHash returns the hash stored at the given height, or "" if absent.
func (s *BboltStore) GetBlockHash(height int64) (string, error) {
	var hash string
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketBlockHash).Get(encHeight(height))
		if v != nil {
			hash = string(v)
		}
		return nil
	})
	return hash, err
}

// GetClassInstalls returns classified SCIDs for a given class, ordered by
// install height ascending. limit<=0 means unlimited.
func (s *BboltStore) GetClassInstalls(class string, limit int) ([]structures.ClassInstall, error) {
	if class == "" {
		return nil, nil
	}
	prefix := append([]byte(class), '|')
	var out []structures.ClassInstall
	err := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketClass).Cursor()
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			// k = "<class>|<BE8 h>|<scid>"
			rem := k[len(prefix):]
			if len(rem) < 8+1 {
				continue
			}
			h := decHeight(rem[:8])
			scid := string(rem[9:])
			var meta structures.ClassMeta
			if err := msgpack.Unmarshal(v, &meta); err != nil {
				logger.Warnf("GetClassInstalls decode %s/%d/%s: %v", class, h, scid, err)
				continue
			}
			out = append(out, structures.ClassInstall{
				SCID:          scid,
				InstallHeight: h,
				Meta:          &meta,
			})
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	return out, err
}

// GetSCIDClass returns the classifier's stored metadata for a SCID, or nil.
func (s *BboltStore) GetSCIDClass(scid string) (*structures.ClassMeta, error) {
	var meta *structures.ClassMeta
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketClassIdx).Get([]byte(scid))
		if v == nil {
			return nil
		}
		var m structures.ClassMeta
		if err := msgpack.Unmarshal(v, &m); err != nil {
			return fmt.Errorf("class_scid decode %s: %w", scid, err)
		}
		meta = &m
		return nil
	})
	return meta, err
}

// GetInstallsInRange returns installs with height in [fromHeight, toHeight).
// limit<=0 means unlimited.
func (s *BboltStore) GetInstallsInRange(fromHeight, toHeight int64, limit int) ([]structures.ClassInstall, error) {
	if toHeight <= fromHeight {
		return nil, nil
	}
	start := encHeight(fromHeight)
	var out []structures.ClassInstall
	err := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketInstalls).Cursor()
		classBucket := tx.Bucket(bucketClassIdx)
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if len(k) < 8+1 {
				continue
			}
			h := decHeight(k[:8])
			if h >= toHeight {
				break
			}
			scid := string(k[9:])
			var rec structures.InstallRecord
			if err := msgpack.Unmarshal(v, &rec); err != nil {
				logger.Warnf("GetInstallsInRange decode h=%d scid=%s: %v", h, scid, err)
				continue
			}
			// Pull class metadata if classified.
			var meta *structures.ClassMeta
			if cv := classBucket.Get([]byte(scid)); cv != nil {
				var m structures.ClassMeta
				if err := msgpack.Unmarshal(cv, &m); err == nil {
					meta = &m
				}
			}
			out = append(out, structures.ClassInstall{
				SCID:          scid,
				InstallHeight: h,
				Meta:          meta,
			})
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	return out, err
}

// GetAddressSCIDs returns the SCIDs an address has interacted with.
// Map value holds first/last interaction heights + count.
func (s *BboltStore) GetAddressSCIDs(addr string) (map[string]*structures.AddrSCIDEntry, error) {
	if addr == "" {
		return nil, nil
	}
	out := make(map[string]*structures.AddrSCIDEntry)
	err := s.DB.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket(bucketAddrSCIDs)
		sub := parent.Bucket([]byte(addr))
		if sub == nil {
			return nil
		}
		return sub.ForEach(func(k, v []byte) error {
			var e structures.AddrSCIDEntry
			var decErr error
			// Tag-byte dispatch: 0x01 is typed v1, 0x80-0x8f is legacy
			// msgpack fixmap. New writes always land as v1.
			if structures.IsAddrSCIDEntryTyped(v) {
				decErr = e.UnmarshalTyped(v)
			} else {
				decErr = msgpack.Unmarshal(v, &e)
			}
			if decErr != nil {
				logger.Warnf("addr_scids decode %s/%s: %v", addr, string(k), decErr)
				return nil
			}
			out[string(k)] = &e
			return nil
		})
	})
	return out, err
}

// hasPrefix is a 2-operand []byte prefix test; avoids bytes.HasPrefix import
// in a module that already imports it indirectly via bbolt.
func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if b[i] != p {
			return false
		}
	}
	return true
}

func (s *BboltStore) GetInvalidSCIDDeploys() (map[string]uint64, error) {
	result := make(map[string]uint64)
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketInvalid).ForEach(func(k, v []byte) error {
			fees, _ := strconv.ParseUint(string(v), 10, 64)
			result[string(k)] = fees
			return nil
		})
	})
	return result, err
}

func (s *BboltStore) GetTxCounts() (reg, burn, norm int64, err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStats)
		parse := func(key string) int64 {
			v := b.Get([]byte(key))
			if v == nil {
				return 0
			}
			n, _ := strconv.ParseInt(string(v), 10, 64)
			return n
		}
		reg = parse("regtxcount")
		burn = parse("burntxcount")
		norm = parse("normtxcount")
		return nil
	})
	return
}

func (s *BboltStore) GetNormalTxWithSCIDByAddr(addr string) ([]*structures.NormalTXWithSCIDParse, error) {
	var txs []*structures.NormalTXWithSCIDParse
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketNormTx).Get([]byte(addr))
		if v == nil {
			return nil
		}
		return msgpack.Unmarshal(v, &txs)
	})
	return txs, err
}
