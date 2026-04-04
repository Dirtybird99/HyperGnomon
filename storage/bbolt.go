package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/hypergnomon/hypergnomon/structures"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var logger = logrus.WithField("pkg", "storage")

// Bucket names
var (
	bucketStats   = []byte("stats")
	bucketOwners  = []byte("owners")
	bucketHeaders = []byte("headers")
	bucketClass   = []byte("class")
	bucketTags    = []byte("tags")
	bucketHeight  = []byte("height")
	bucketScVars  = []byte("scvars")
	bucketNormTx  = []byte("normaltxwithscid")
	bucketInvalid = []byte("invalidscidinvokes")
)

// BboltStore implements Storage backed by BoltDB.
type BboltStore struct {
	DB   *bolt.DB
	Path string
	mu   sync.Mutex // Proper mutex, not the 20ms spin-lock from original Gnomon
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
	s.DB.Sync()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketStats).Put(
			[]byte("lastindexedheight"),
			[]byte(strconv.FormatInt(height, 10)),
		)
	})
}

func (s *BboltStore) StoreOwner(scid, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(scid))
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%s:%d:%s", sender, details.Txid[:8], height, entrypoint)
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
		return msgpack.Unmarshal(v, &vars)
	})
	return vars, err
}

func (s *BboltStore) StoreSCIDInteractionHeight(scid string, height int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketHeight)
		existing := b.Get([]byte(scid))
		var heights []int64
		if existing != nil {
			msgpack.Unmarshal(existing, &heights)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNormTx)
		existing := b.Get([]byte(addr))
		var txs []*structures.NormalTXWithSCIDParse
		if existing != nil {
			msgpack.Unmarshal(existing, &txs)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketInvalid).Put([]byte(scid), []byte(strconv.FormatUint(fees, 10)))
	})
}

func (s *BboltStore) StoreTxCounts(reg, burn, norm int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.DB.Update(func(tx *bolt.Tx) error {
		// Store owners
		ownerBucket := tx.Bucket(bucketOwners)
		for scid, owner := range batch.Owners {
			if err := ownerBucket.Put([]byte(scid), []byte(owner)); err != nil {
				return fmt.Errorf("batch owner %s: %w", scid, err)
			}
		}

		// Store invocations
		for _, inv := range batch.Invocations {
			b, err := tx.CreateBucketIfNotExists([]byte(inv.Scid))
			if err != nil {
				return fmt.Errorf("batch invoke bucket %s: %w", inv.Scid, err)
			}
			key := fmt.Sprintf("%s:%s:%d:%s", inv.Sender, inv.Details.Txid[:8], inv.Height, inv.Entrypoint)
			val, err := msgpack.Marshal(inv.Details)
			if err != nil {
				return fmt.Errorf("batch invoke marshal: %w", err)
			}
			if err := b.Put([]byte(key), val); err != nil {
				return err
			}
		}

		// Store variables
		varBucket := tx.Bucket(bucketScVars)
		for scid, heightVars := range batch.Variables {
			for height, vars := range heightVars {
				key := fmt.Sprintf("%s:%d", scid, height)
				val, err := msgpack.Marshal(vars)
				if err != nil {
					return fmt.Errorf("batch vars marshal: %w", err)
				}
				if err := varBucket.Put([]byte(key), val); err != nil {
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
				msgpack.Unmarshal(existing, &current)
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
				msgpack.Unmarshal(existing, &current)
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

		// Store last indexed height (crash recovery point)
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
