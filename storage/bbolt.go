package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

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
	bucketClass   = []byte("class") // Route B: "<class>|<BE8:h>|<scid>" -> ClassMeta (typed v1 tag 0x04; legacy msgpack fixmap reads via decodeClassMeta)
	bucketTags    = []byte("tags")  // reserved; populated when tag indexes ship
	bucketHeight  = []byte("height")
	bucketScVars  = []byte("scvars")
	// bucketScVarsLatest stores scid -> BE8 latest snapshot height. It avoids
	// prefix-scanning every scvars snapshot for latest-rating reads.
	bucketScVarsLatest = []byte("scvars_latest")
	bucketNormTx       = []byte("normaltxwithscid")
	bucketInvalid      = []byte("invalidscidinvokes")

	// Route B (DESIGN.md §3) buckets.
	bucketBlockHash   = []byte("blockhashes")  // BE8 height -> 64-hex block hash
	bucketInstalls    = []byte("installs")     // "<BE8:h>|<scid>" -> InstallRecord msgpack
	bucketClassIdx    = []byte("class_scid")   // scid -> ClassMeta (typed v1 tag 0x04; legacy msgpack reads via decodeClassMeta; O(1) lookup)
	bucketAddrSCIDs   = []byte("addr_scids")   // parent; per-addr sub-bucket created on demand
	bucketTELAContent = []byte("tela_content") // scid|path -> {body, mime, sha256}
	bucketSCCode      = []byte("sccode")       // scid -> SCCodeEntry (install-time DVM code)
	bucketOwnerSCIDs  = []byte("owner_scids")  // "<owner>|<scid>" -> "" (prefix-scan owner → [scid])
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
			bucketScVars, bucketScVarsLatest, bucketNormTx, bucketInvalid,
			// Route B M0
			bucketBlockHash, bucketInstalls, bucketClassIdx,
			bucketAddrSCIDs, bucketTELAContent, bucketSCCode,
			bucketOwnerSCIDs,
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

	logger.Debugf("BoltDB opened: %s", dbPath)
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
		owners := tx.Bucket(bucketOwners)
		isNew := owners.Get([]byte(scid)) == nil
		if err := owners.Put([]byte(scid), []byte(owner)); err != nil {
			return err
		}
		if owner != "" {
			if err := tx.Bucket(bucketOwnerSCIDs).Put(ownerSCIDKey(owner, scid), nil); err != nil {
				return err
			}
		}
		if isNew {
			return addSCIDCountStat(tx, 1)
		}
		return nil
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
		if err := b.Put([]byte(key), val); err != nil {
			return err
		}
		return putLatestSCVarsHeight(tx.Bucket(bucketScVarsLatest), scid, height)
	})
}

func (s *BboltStore) GetSCIDVariableDetailsAtHeight(scid string, height int64) ([]*structures.SCIDVariable, error) {
	var vars []*structures.SCIDVariable
	err := s.DB.View(func(tx *bolt.Tx) error {
		var err error
		vars, err = getSCIDVariablesAtHeightTx(tx, scid, height)
		return err
	})
	return vars, err
}

func getSCIDVariablesAtHeightTx(tx *bolt.Tx, scid string, height int64) ([]*structures.SCIDVariable, error) {
	if scid == "" || height <= 0 {
		return nil, nil
	}
	key := fmt.Sprintf("%s:%d", scid, height)
	v := tx.Bucket(bucketScVars).Get([]byte(key))
	if v == nil {
		return nil, nil
	}
	return decodeSCIDVariables(v)
}

func decodeSCIDVariables(v []byte) ([]*structures.SCIDVariable, error) {
	if structures.IsSCIDVariablesTyped(v) {
		return structures.UnmarshalSCIDVariablesTyped(v)
	}
	var vars []*structures.SCIDVariable
	if err := msgpack.Unmarshal(v, &vars); err != nil {
		return nil, err
	}
	return vars, nil
}

func latestSCVarsHeightTx(tx *bolt.Tx, scid string) int64 {
	if scid == "" {
		return 0
	}
	if v := tx.Bucket(bucketScVarsLatest).Get([]byte(scid)); len(v) >= 8 {
		return decHeight(v)
	}
	prefix := []byte(scid + ":")
	var maxH int64
	c := tx.Bucket(bucketScVars).Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		h, err := strconv.ParseInt(string(k[len(prefix):]), 10, 64)
		if err != nil {
			continue
		}
		if h > maxH {
			maxH = h
		}
	}
	return maxH
}

func putLatestSCVarsHeight(b *bolt.Bucket, scid string, height int64) error {
	if scid == "" || height <= 0 {
		return nil
	}
	if existing := b.Get([]byte(scid)); len(existing) >= 8 && decHeight(existing) >= height {
		return nil
	}
	val := make([]byte, 8)
	copy(val, encHeight(height))
	return b.Put([]byte(scid), val)
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

func addIntStat(b *bolt.Bucket, key string, delta int64) error {
	if delta == 0 {
		return nil
	}
	existing := b.Get([]byte(key))
	var current int64
	if existing != nil {
		current, _ = strconv.ParseInt(string(existing), 10, 64)
	}
	return b.Put([]byte(key), []byte(strconv.FormatInt(current+delta, 10)))
}

func addSCIDCountStat(tx *bolt.Tx, delta int64) error {
	if delta == 0 {
		return nil
	}
	stats := tx.Bucket(bucketStats)
	key := []byte("sc_count")
	if existing := stats.Get(key); existing != nil {
		n, _ := strconv.ParseInt(string(existing), 10, 64)
		return stats.Put(key, []byte(strconv.FormatInt(n+delta, 10)))
	}
	var total int64
	if err := tx.Bucket(bucketOwners).ForEach(func(_, _ []byte) error {
		total++
		return nil
	}); err != nil {
		return err
	}
	return stats.Put(key, []byte(strconv.FormatInt(total, 10)))
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

func (s *BboltStore) GetSCIDCount() (int64, error) {
	var count int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketStats).Get([]byte("sc_count")); v != nil {
			n, err := strconv.ParseInt(string(v), 10, 64)
			if err == nil {
				count = n
				return nil
			}
		}
		var n int64
		if err := tx.Bucket(bucketOwners).ForEach(func(_, _ []byte) error {
			n++
			return nil
		}); err != nil {
			return err
		}
		count = n
		return nil
	})
	return count, err
}

func (s *BboltStore) GetOwnersForSCIDs(scids []string) (map[string]string, error) {
	out := make(map[string]string, len(scids))
	if len(scids) == 0 {
		return out, nil
	}
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOwners)
		for _, scid := range scids {
			if scid == "" {
				continue
			}
			if v := b.Get([]byte(scid)); v != nil {
				out[scid] = string(v)
			}
		}
		return nil
	})
	return out, err
}

// FlushBatch atomically writes all accumulated data in a single BoltDB transaction.
// This is the arena-pattern payoff: one lock acquisition for potentially thousands of records.
func (s *BboltStore) FlushBatch(batch *WriteBatch) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		// Store owners
		ownerBucket := tx.Bucket(bucketOwners)
		var newOwners int64
		for scid, owner := range batch.Owners {
			if ownerBucket.Get([]byte(scid)) == nil {
				newOwners++
			}
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
		//
		// CRITICAL: each Put needs its OWN value buffer. bbolt stores the
		// slice header during Put and only copies bytes at commit time, so
		// reusing a single backing array across Puts corrupts all but the
		// last write (earlier nodes end up pointing at mutated bytes).
		// Keys are safe to reuse because bolt's bucket.Put copies keys
		// eagerly (node.put clones the key into its own arena).
		varBucket := tx.Bucket(bucketScVars)
		varLatestBucket := tx.Bucket(bucketScVarsLatest)
		for scid, heightVars := range batch.Variables {
			for height, vars := range heightVars {
				keyBuf = keyBuf[:0]
				keyBuf = append(keyBuf, scid...)
				keyBuf = append(keyBuf, ':')
				keyBuf = strconv.AppendInt(keyBuf, height, 10)
				val := structures.MarshalSCIDVariablesTypedAppend(nil, vars)
				if err := varBucket.Put(keyBuf, val); err != nil {
					return err
				}
				if err := putLatestSCVarsHeight(varLatestBucket, scid, height); err != nil {
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
		if err := addSCIDCountStat(tx, newOwners); err != nil {
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
		//
		// classPrefix is keyed by height, so re-probing the same SCID at a
		// new chain height would leave a stale entry at the old height and
		// make GetClassInstalls return that SCID twice. Before writing the
		// new key, look up the current classIdx record for this SCID: if its
		// old (class, installHeight) differs from the new one we're about to
		// write, delete the stale classPrefix key so each SCID stays
		// represented exactly once in the class bucket.
		if len(batch.Classes) > 0 {
			classIdx := tx.Bucket(bucketClassIdx)
			classPrefix := tx.Bucket(bucketClass)
			for scid, meta := range batch.Classes {
				if meta == nil || meta.Class == "" {
					continue
				}
				// Preflight: if we're overwriting a record at a different
				// (class, InstallHeight), delete the stale classPrefix row
				// so each SCID appears exactly once in a class scan.
				// decodeClassMeta transparently handles both typed v1 and
				// legacy msgpack records during the transition window.
				if prev := classIdx.Get([]byte(scid)); prev != nil {
					if oldMeta, err := decodeClassMeta(prev); err == nil && oldMeta != nil {
						if oldMeta.Class != meta.Class || oldMeta.InstallHeight != meta.InstallHeight {
							_ = classPrefix.Delete(classKey(oldMeta.Class, oldMeta.InstallHeight, scid))
						}
					}
				}
				// Typed v1 write — 1 alloc for the buffer, no msgpack
				// reflection. Same slice serves both Puts (bbolt needs the
				// bytes valid until commit, not unique per Put).
				val := meta.MarshalTyped()
				if err := classIdx.Put([]byte(scid), val); err != nil {
					return err
				}
				if err := classPrefix.Put(classKey(meta.Class, meta.InstallHeight, scid), val); err != nil {
					return err
				}
			}
		}

		// SC code snapshots: scid -> SCCodeEntry. Write-once semantics
		// (DERO code is immutable), so we just Put unconditionally — any
		// overwrite is a no-op byte-wise. Typed v1 encoding: 1 alloc
		// per entry (the marshal buffer), zero framing overhead. Msgpack
		// baseline paid 3 allocs + ~16% header bloat.
		//
		// bbolt.Put requires the supplied value to stay valid for the
		// life of the transaction, so we can't reuse a single scratch
		// buffer across iterations — fresh allocation matches the Classes
		// / Installs loops above.
		if len(batch.SCCodes) > 0 {
			codeBucket := tx.Bucket(bucketSCCode)
			for scid, entry := range batch.SCCodes {
				if entry == nil || entry.Code == "" {
					continue
				}
				if err := codeBucket.Put([]byte(scid), entry.MarshalTyped()); err != nil {
					return err
				}
			}
		}

		// Owner → SCIDs reverse index: populated from batch.OwnerSCIDs so
		// listsc_byowner / getscidlist_byaddr don't need a full-owners scan.
		// Value is empty (key-only); the key layout is "<owner>|<scid>" so
		// a prefix scan returns every scid for the given owner.
		if len(batch.OwnerSCIDs) > 0 {
			osBucket := tx.Bucket(bucketOwnerSCIDs)
			for owner, scids := range batch.OwnerSCIDs {
				if owner == "" {
					continue
				}
				for scid := range scids {
					if err := osBucket.Put(ownerSCIDKey(owner, scid), nil); err != nil {
						return err
					}
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
			// Each Put stores the slice header; bolt copies to the page only
			// at commit. Using a single shared backing array across Puts
			// would leave all earlier-Put'd slice headers pointing at the
			// last iteration's bytes. Allocate one slice per Put.
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
					if err := subBucket.Put([]byte(scid), merged.MarshalTyped()); err != nil {
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

// decodeClassMeta dispatches on the leading tag byte to pick between the
// v1 typed decoder and the legacy msgpack decoder. Legacy records upgrade
// in-place on next write (FlushBatch always emits v1). Returned meta is a
// fresh allocation — callers own it.
func decodeClassMeta(v []byte) (*structures.ClassMeta, error) {
	if len(v) == 0 {
		return nil, nil
	}
	var m structures.ClassMeta
	if structures.IsClassMetaTyped(v) {
		if err := m.UnmarshalTyped(v); err != nil {
			return nil, err
		}
		return &m, nil
	}
	if err := msgpack.Unmarshal(v, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// GetClassInstalls returns classified SCIDs for a given class, one entry
// per unique SCID (the highest-height entry wins). Ordered by install
// height ascending. limit<=0 means unlimited.
//
// Dedup note: classPrefix is keyed by (class, height, scid), so a SCID
// re-indexed at a later chain height used to leak two+ rows. FlushBatch
// now deletes stale rows before writing new ones, but legacy DBs can
// still contain duplicates — we collapse them here at read time so every
// caller (GetTELAApps, GetMyDOCs, OmniSearch, …) sees a clean view.
func (s *BboltStore) GetClassInstalls(class string, limit int) ([]structures.ClassInstall, error) {
	if class == "" {
		return nil, nil
	}
	prefix := append([]byte(class), '|')
	latest := make(map[string]structures.ClassInstall)
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
			meta, err := decodeClassMeta(v)
			if err != nil {
				logger.Warnf("GetClassInstalls decode %s/%d/%s: %v", class, h, scid, err)
				continue
			}
			if prev, seen := latest[scid]; !seen || h > prev.InstallHeight {
				latest[scid] = structures.ClassInstall{
					SCID:          scid,
					InstallHeight: h,
					Meta:          meta,
				}
			}
		}
		return nil
	})
	// Emit in install-height-ascending order so callers can treat limit as
	// "oldest N" just like before the dedup.
	out := make([]structures.ClassInstall, 0, len(latest))
	for _, inst := range latest {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstallHeight < out[j].InstallHeight
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}

// GetSCIDClass returns the classifier's stored metadata for a SCID, or nil.
// Dispatches on the leading tag byte so legacy msgpack records still decode
// until they're rewritten as typed v1 on the next class update.
func (s *BboltStore) GetSCIDClass(scid string) (*structures.ClassMeta, error) {
	var meta *structures.ClassMeta
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketClassIdx).Get([]byte(scid))
		if v == nil {
			return nil
		}
		m, err := decodeClassMeta(v)
		if err != nil {
			return fmt.Errorf("class_scid decode %s: %w", scid, err)
		}
		meta = m
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
			// Pull class metadata if classified (typed or legacy).
			var meta *structures.ClassMeta
			if cv := classBucket.Get([]byte(scid)); cv != nil {
				if m, err := decodeClassMeta(cv); err == nil {
					meta = m
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

// GetRatingsForSCID returns the TELA ratings stored as variables on scid at
// the given snapshot height. height <= 0 picks the latest snapshot.
//
// Per the canonical TELA spec (github.com/civilware/tela):
//   - The `Rate(r uint64)` entrypoint stores each rater's entry under a
//     STORE key that **is** the rater's wallet address (e.g. "deto1qy…"),
//     not a "rating_<addr>" prefix.
//   - The value is hex-encoded `"<score>_<height>"`, e.g. "35_6937500" →
//     hex "33355f36393337353030".
//   - Score is 0–99; >=50 → like, <50 → dislike (aggregate keys `likes`
//     and `dislikes` count these separately).
//   - There is no `ratingMsg_<addr>` companion key; ratings carry no
//     message payload.
//
// Returns ([], nil) when the SCID has no ratings, or an error on a DB issue.
func (s *BboltStore) GetRatingsForSCID(scid string, height int64) ([]structures.Rating, error) {
	ratings, _, err := s.GetRatingsAndSummaryForSCID(scid, height)
	return ratings, err
}

// GetRatingSummary reads the `likes` and `dislikes` aggregate STORE keys
// that the TELA `Rate()` entrypoint maintains alongside the per-rater
// entries. Returns zero values if neither key is present.
func (s *BboltStore) GetRatingSummary(scid string, height int64) (*structures.RatingSummary, error) {
	_, summary, err := s.GetRatingsAndSummaryForSCID(scid, height)
	return summary, err
}

func (s *BboltStore) GetRatingsAndSummaryForSCID(scid string, height int64) ([]structures.Rating, *structures.RatingSummary, error) {
	if scid == "" {
		return nil, nil, nil
	}

	var ratings []structures.Rating
	var summary *structures.RatingSummary
	err := s.DB.View(func(tx *bolt.Tx) error {
		resolvedHeight := height
		if resolvedHeight <= 0 {
			resolvedHeight = latestSCVarsHeightTx(tx, scid)
		}
		if resolvedHeight <= 0 {
			summary = &structures.RatingSummary{Height: 0}
			return nil
		}
		vars, err := getSCIDVariablesAtHeightTx(tx, scid, resolvedHeight)
		if err != nil {
			return err
		}
		summary = &structures.RatingSummary{Height: resolvedHeight}
		ratings = ratingsFromVars(vars, resolvedHeight, summary)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return ratings, summary, nil
}

func ratingsFromVars(vars []*structures.SCIDVariable, height int64, sum *structures.RatingSummary) []structures.Rating {
	if len(vars) == 0 {
		return nil
	}
	out := make([]structures.Rating, 0, 4)
	for _, v := range vars {
		keyStr, ok := v.Key.(string)
		if !ok {
			continue
		}
		switch keyStr {
		case "likes":
			if sum != nil {
				sum.Likes = uint64(ratingScore(v.Value))
			}
			continue
		case "dislikes":
			if sum != nil {
				sum.Dislikes = uint64(ratingScore(v.Value))
			}
			continue
		}
		if !looksLikeDEROAddress(keyStr) {
			continue
		}
		score, h, ok := parseTELARatingValue(v.Value)
		if !ok {
			continue
		}
		if h == 0 {
			h = height
		}
		out = append(out, structures.Rating{
			Rater:  keyStr,
			Score:  score,
			Height: h,
		})
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rater < out[j].Rater })
	return out
}

// looksLikeDEROAddress is a cheap shape check: DERO mainnet addresses
// start with "dero1" and are ~66 chars Base58 (testnet "deto1"). We just
// prefix-check + length-gate here; callers don't need strict checksum
// validation, only "not a regular English variable name."
func looksLikeDEROAddress(s string) bool {
	if len(s) < 40 || len(s) > 128 {
		return false
	}
	if strings.HasPrefix(s, "dero1") || strings.HasPrefix(s, "deto1") || strings.HasPrefix(s, "deroi1") {
		return true
	}
	return false
}

// parseTELARatingValue decodes the TELA rating value format: the stored
// value is hex-encoded `"<score>_<height>"`. Returns (score, height, ok).
// ok=false means the value doesn't match the rating format at all.
func parseTELARatingValue(raw interface{}) (float64, int64, bool) {
	s, ok := raw.(string)
	if !ok {
		return 0, 0, false
	}
	// Best-effort hex decode; TELA stores the value as hex bytes of the
	// ASCII "<score>_<height>" string. Legacy records may already be raw
	// ASCII, so fall through if hex-decode fails.
	decoded := hexDecodeIfHex(s)
	parts := strings.SplitN(decoded, "_", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	scoreF, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, 0, false
	}
	if scoreF < 0 || scoreF >= 100 {
		return 0, 0, false
	}
	height, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return scoreF, height, true
}

// hexDecodeIfHex returns the hex-decoded bytes of s if s is a valid hex
// string, otherwise s unchanged. The TELA Rate() entrypoint stores rating
// values as hex-encoded ASCII, so real on-chain data will always hex-decode;
// this fallback is defensive for test fixtures that pre-decode.
func hexDecodeIfHex(s string) string {
	if len(s)%2 != 0 {
		return s
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F') {
			return s
		}
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi := hexNibble(s[i*2])
		lo := hexNibble(s[i*2+1])
		out[i] = hi<<4 | lo
	}
	return string(out)
}

func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// ResetIndex drops every data bucket then recreates them empty. The block
// hash bucket is preserved so reorg-detection has history to compare against
// when the next scan reaches those heights again. Safe to call on an open
// DB; callers should ensure no scan is running.
func (s *BboltStore) ResetIndex() error {
	toDrop := [][]byte{
		bucketStats, bucketOwners, bucketHeaders,
		bucketClass, bucketTags, bucketHeight,
		bucketScVars, bucketScVarsLatest, bucketNormTx, bucketInvalid,
		bucketInstalls, bucketClassIdx,
		bucketAddrSCIDs, bucketTELAContent, bucketSCCode,
		bucketOwnerSCIDs,
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		for _, name := range toDrop {
			if tx.Bucket(name) != nil {
				if err := tx.DeleteBucket(name); err != nil {
					return fmt.Errorf("drop bucket %s: %w", name, err)
				}
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return fmt.Errorf("recreate bucket %s: %w", name, err)
			}
		}
		return nil
	})
}

// GetSCIDsByOwner returns every SCID installed by the given owner address.
// Prefix-scans the owner_scids bucket; linear in the count of SCIDs for
// this owner (not the total — bbolt's cursor advances inside a shared page
// as long as keys share the prefix).
//
// Ordering is by SCID bytewise ascending (bbolt's natural key order).
// Returns ([], nil) on unknown owner.
func (s *BboltStore) GetSCIDsByOwner(owner string) ([]string, error) {
	if owner == "" {
		return nil, nil
	}
	prefix := append([]byte(owner), '|')
	var out []string
	err := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketOwnerSCIDs).Cursor()
		for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
			scid := string(k[len(prefix):])
			if scid != "" {
				out = append(out, scid)
			}
		}
		return nil
	})
	return out, err
}

// ownerSCIDKey packs owner + scid for the flat bucket layout.
func ownerSCIDKey(owner, scid string) []byte {
	k := make([]byte, 0, len(owner)+1+len(scid))
	k = append(k, owner...)
	k = append(k, '|')
	k = append(k, scid...)
	return k
}

// GetSCCode returns the install-time code snapshot for scid. (nil, nil) on
// miss — caller (indexer.GetSCCode) handles lazy backfill from the daemon.
// Uses the v1 typed encoding — one string allocation for the Code, zero
// header allocations. The msgpack baseline was 3 allocs + ~16% framing
// overhead on 2 KB contracts.
func (s *BboltStore) GetSCCode(scid string) (*structures.SCCodeEntry, error) {
	if scid == "" {
		return nil, nil
	}
	var out *structures.SCCodeEntry
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketSCCode).Get([]byte(scid))
		if v == nil {
			return nil
		}
		var e structures.SCCodeEntry
		if err := e.UnmarshalTyped(v); err != nil {
			return err
		}
		out = &e
		return nil
	})
	return out, err
}

// PutSCCode writes a code snapshot. Overwrites any previous entry for scid.
// Normal install/lazy-backfill path is WriteBatch.AddSCCode + FlushBatch —
// this direct method exists for tests and the lazy-backfill cache-fill path
// in indexer.GetSCCode where a one-shot write is cheaper than a pooled batch.
func (s *BboltStore) PutSCCode(scid string, entry *structures.SCCodeEntry) error {
	if scid == "" || entry == nil {
		return nil
	}
	val := entry.MarshalTyped()
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSCCode).Put([]byte(scid), val)
	})
}

// telaContentKey packs (scid, path) into a single bucket key. '|' is a safe
// separator because SCIDs are 64-hex and TELA paths never contain '|'.
func telaContentKey(scid, path string) []byte {
	k := make([]byte, 0, len(scid)+1+len(path))
	k = append(k, scid...)
	k = append(k, '|')
	k = append(k, path...)
	return k
}

// GetTELAContent reads a cached TELA content entry. Returns (nil, nil) on miss.
func (s *BboltStore) GetTELAContent(scid, path string) (*structures.TELAContentEntry, error) {
	if scid == "" {
		return nil, nil
	}
	var out *structures.TELAContentEntry
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketTELAContent).Get(telaContentKey(scid, path))
		if v == nil {
			return nil
		}
		var e structures.TELAContentEntry
		if err := msgpack.Unmarshal(v, &e); err != nil {
			return err
		}
		out = &e
		return nil
	})
	return out, err
}

// PutTELAContent writes a content entry. Overwrites any previous entry for
// the same (scid, path). Caller should have already computed ETag.
func (s *BboltStore) PutTELAContent(scid, path string, entry *structures.TELAContentEntry) error {
	if scid == "" || entry == nil {
		return nil
	}
	val, err := msgpack.Marshal(entry)
	if err != nil {
		return err
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTELAContent).Put(telaContentKey(scid, path), val)
	})
}

// DeleteTELAContentForSCID removes every cached entry for the given SCID.
// Called from the bus invalidator when EventInstall / EventVarChange fires
// on a TELA app — the content may have been updated on-chain.
func (s *BboltStore) DeleteTELAContentForSCID(scid string) error {
	if scid == "" {
		return nil
	}
	prefix := append([]byte(scid), '|')
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTELAContent)
		c := b.Cursor()
		var toDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
			// Copy — bbolt key slices are only valid inside the callback.
			kc := make([]byte, len(k))
			copy(kc, k)
			toDelete = append(toDelete, kc)
		}
		for _, k := range toDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// ratingScore converts a rating variable value to float64. TELA contracts
// store ratings as uint64 / int / string depending on SDK version — tolerate
// all three and return 0 for unparseable values (caller can filter).
func ratingScore(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	}
	return 0
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
