package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

const classifySeedCacheVersion = 1

type classifySeedCache struct {
	Version      int                              `msgpack:"v"`
	Network      string                           `msgpack:"network"`
	GnomonSCID   string                           `msgpack:"gnomon"`
	RegistryHash string                           `msgpack:"registry_hash"`
	Height       int64                            `msgpack:"height"`
	Timestamp    int64                            `msgpack:"ts"`
	IndexSCIDs   []string                         `msgpack:"index"`
	DocSCIDs     []string                         `msgpack:"doc"`
	OtherClasses map[string][]string              `msgpack:"classes,omitempty"`
	Classes      map[string]*structures.ClassMeta `msgpack:"class_meta"`
	Variables    map[string]classifySeedVars      `msgpack:"vars,omitempty"`
	Codes        map[string]classifySeedCode      `msgpack:"codes,omitempty"`
}

type classifySeedVars struct {
	Height int64                      `msgpack:"h"`
	Vars   []*structures.SCIDVariable `msgpack:"vars"`
}

type classifySeedCode struct {
	Height int64  `msgpack:"h"`
	Code   string `msgpack:"code"`
}

type classifySeedProbeContext struct {
	Network      string
	GnomonSCID   string
	RegistryHash string
}

func classifySeedRegistryHash(candidates []*registryEntry, maxHeight int64) string {
	type row struct {
		scid   string
		owner  string
		height int64
	}
	rows := make([]row, 0, len(candidates))
	for _, c := range candidates {
		if c == nil {
			continue
		}
		if maxHeight > 0 && c.height > maxHeight {
			continue
		}
		rows = append(rows, row{scid: c.scid, owner: c.owner, height: c.height})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].scid < rows[j].scid
	})
	h := sha256.New()
	for _, r := range rows {
		_, _ = fmt.Fprintf(h, "%s\x00%s\x00%d\n", r.scid, r.owner, r.height)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func defaultClassifySeedCacheDir() string {
	if root, err := os.UserCacheDir(); err == nil && root != "" {
		return filepath.Join(root, "HyperGnomon", "classify_seed")
	}
	return filepath.Join(os.TempDir(), "HyperGnomon", "classify_seed")
}

func classifySeedCacheDir(idx *Indexer) string {
	if idx != nil && idx.ClassifySeedCacheDir != "" {
		return idx.ClassifySeedCacheDir
	}
	return defaultClassifySeedCacheDir()
}

func classifySeedCacheFileName(cache *classifySeedCache) string {
	hash := cache.RegistryHash
	if len(hash) > 16 {
		hash = hash[:16]
	}
	return fmt.Sprintf("%s-v%d-%s-h%d-%s.bin",
		sanitizeSeedCachePart(cache.Network),
		cache.Version,
		shortSCID(cache.GnomonSCID),
		cache.Height,
		hash,
	)
}

func sanitizeSeedCachePart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(s)
}

func shortSCID(scid string) string {
	if len(scid) >= 16 {
		return scid[:16]
	}
	return sanitizeSeedCachePart(scid)
}

// classifySeedCacheKeepCount is how many seed files survive pruning per
// (network, gnomonSCID) pair. Newest-by-height wins; keeping two leaves one
// fallback in case the newest seed was written against a registry state the
// chain has since reorged away from.
const classifySeedCacheKeepCount = 2

func saveClassifySeedCache(dir string, cache *classifySeedCache) error {
	if cache == nil {
		return fmt.Errorf("nil classify seed cache")
	}
	if cache.Version == 0 {
		cache.Version = classifySeedCacheVersion
	}
	if cache.Timestamp == 0 {
		cache.Timestamp = time.Now().Unix()
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	data, err := msgpack.Marshal(cache)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, classifySeedCacheFileName(cache))
	// os.CreateTemp picks a random unique name (mode 0600), so two probes
	// racing on the same directory can never clobber each other's half-
	// written temp file the way the old deterministic path+".tmp" could.
	tmpFile, err := os.CreateTemp(dir, "classify-seed-*.tmp")
	if err != nil {
		return err
	}
	tmp := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(path)
		if err2 := os.Rename(tmp, path); err2 != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	// Seed files are one-per-(height,hash); without pruning they accumulate
	// forever. Keep the newest classifySeedCacheKeepCount for this
	// (network, gnomonSCID) pair. Prune failure is non-fatal — the save
	// itself succeeded.
	if err := pruneClassifySeedCaches(dir, cache.Network, cache.GnomonSCID, classifySeedCacheKeepCount); err != nil {
		logger.Debugf("classify seed cache prune: %v", err)
	}
	return nil
}

// pruneClassifySeedCaches removes all but the newest `keep` seed files (by
// embedded height, ties broken by name) belonging to the given
// (network, gnomonSCID) pair. Files for other networks/registries — and
// anything whose name doesn't parse as one of ours — are left untouched.
func pruneClassifySeedCaches(dir, network, gnomonSCID string, keep int) error {
	if keep < 1 {
		keep = 1
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	prefix := sanitizeSeedCachePart(network) + "-v"
	marker := "-" + shortSCID(gnomonSCID) + "-h"
	type seedFile struct {
		name   string
		height int64
	}
	var files []seedFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".bin") || !strings.HasPrefix(name, prefix) {
			continue
		}
		hPos := strings.Index(name, marker)
		if hPos < 0 {
			continue
		}
		rest := name[hPos+len(marker):]
		dash := strings.IndexByte(rest, '-')
		if dash <= 0 {
			continue
		}
		height, err := strconv.ParseInt(rest[:dash], 10, 64)
		if err != nil {
			continue
		}
		files = append(files, seedFile{name: name, height: height})
	}
	if len(files) <= keep {
		return nil
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].height != files[j].height {
			return files[i].height > files[j].height
		}
		return files[i].name > files[j].name
	})
	var firstErr error
	for _, f := range files[keep:] {
		if err := os.Remove(filepath.Join(dir, f.name)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func loadMatchingClassifySeedCache(dir, network, gnomonSCID string, candidates []*registryEntry) (*classifySeedCache, []*registryEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var best *classifySeedCache
	var bestDelta []*registryEntry
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bin") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name())) // #nosec G304 -- path is from the selected cache directory.
		if err != nil {
			continue
		}
		var cache classifySeedCache
		if err := msgpack.Unmarshal(data, &cache); err != nil {
			continue
		}
		if cache.Version != classifySeedCacheVersion || cache.Network != network || cache.GnomonSCID != gnomonSCID || cache.Height <= 0 {
			continue
		}
		if classifySeedRegistryHash(candidates, cache.Height) != cache.RegistryHash {
			continue
		}
		if best != nil && cache.Height <= best.Height {
			continue
		}
		best = &cache
		bestDelta = deltaCandidatesSince(candidates, cache.Height)
	}
	return best, bestDelta, nil
}

func deltaCandidatesSince(candidates []*registryEntry, height int64) []*registryEntry {
	var delta []*registryEntry
	for _, c := range candidates {
		if c != nil && c.height > height {
			delta = append(delta, c)
		}
	}
	return delta
}

func (idx *Indexer) applyClassifySeedCache(cache *classifySeedCache) error {
	if idx == nil || idx.Store == nil || cache == nil {
		return nil
	}
	batch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(batch)

	for scid, meta := range cache.Classes {
		if scid == "" || meta == nil || meta.Class == "" {
			continue
		}
		batch.AddClass(scid, meta)
	}
	for scid, snapshot := range cache.Variables {
		if scid == "" || snapshot.Height <= 0 || len(snapshot.Vars) == 0 {
			continue
		}
		batch.AddVariables(scid, snapshot.Height, snapshot.Vars)
	}
	for scid, code := range cache.Codes {
		if scid == "" || code.Code == "" {
			continue
		}
		class := ""
		if meta := cache.Classes[scid]; meta != nil {
			class = meta.Class
		}
		if idx.shouldPersistCode(class) {
			batch.AddSCCode(scid, code.Height, code.Code)
		}
	}
	if err := idx.Store.FlushBatch(batch); err != nil {
		return err
	}
	if err := saveTELACache(idx.DBDir, cache.IndexSCIDs, cache.DocSCIDs, cache.OtherClasses, cache.Height); err != nil {
		logger.Warnf("classify seed local tela cache save: %v", err)
	}
	structures.TELACount.Store(int64(len(cache.IndexSCIDs) + len(cache.DocSCIDs)))
	// Settlement (TELAProbeSettled) is the CALLER's concern: the fastsync seed
	// branch settles or defers to its delta probe after this returns.
	return nil
}

// trueLocalCacheMissSeed consults the cross-DB classify seed cache ONLY when
// the local TELA cache was a TRUE miss (loadTELACache returned nothing).
// A local cache that loaded but was version-rejected or failed the scvars
// sanity probe must take FastSync's full-probe self-heal path instead —
// applying a seed there would leave corrupt historical scvars blobs in
// place while claiming the classify work is done.
func trueLocalCacheMissSeed(idx *Indexer, trueCacheMiss bool, network, gnomonSCID string, candidates []*registryEntry) ([]*registryEntry, bool) {
	if !trueCacheMiss {
		return nil, false
	}
	return idx.tryApplyClassifySeedCache(network, gnomonSCID, candidates)
}

func (idx *Indexer) tryApplyClassifySeedCache(network, gnomonSCID string, candidates []*registryEntry) ([]*registryEntry, bool) {
	dir := classifySeedCacheDir(idx)
	cache, delta, err := loadMatchingClassifySeedCache(dir, network, gnomonSCID, candidates)
	if err != nil {
		logger.Warnf("Classify seed cache load: %v", err)
		return nil, false
	}
	if cache == nil {
		return nil, false
	}
	if err := idx.applyClassifySeedCache(cache); err != nil {
		logger.Warnf("Classify seed cache apply: %v", err)
		return nil, false
	}
	otherCount := 0
	for _, scids := range cache.OtherClasses {
		otherCount += len(scids)
	}
	logger.Infof("Classify seed cache hit: %d INDEX + %d DOC + %d other classes from height %d (%d delta candidates)",
		len(cache.IndexSCIDs), len(cache.DocSCIDs), otherCount, cache.Height, len(delta))
	return delta, true
}

func newClassifySeedCacheFromProbe(network, gnomonSCID, registryHash string, height int64, indexSCIDs, docSCIDs []string, otherClasses map[string][]string, codeBatch, varBatch *storage.WriteBatch) *classifySeedCache {
	cache := &classifySeedCache{
		Version:      classifySeedCacheVersion,
		Network:      network,
		GnomonSCID:   gnomonSCID,
		RegistryHash: registryHash,
		Height:       height,
		Timestamp:    time.Now().Unix(),
		IndexSCIDs:   append([]string(nil), indexSCIDs...),
		DocSCIDs:     append([]string(nil), docSCIDs...),
		OtherClasses: copyClassMap(otherClasses),
		Classes:      make(map[string]*structures.ClassMeta),
		Variables:    make(map[string]classifySeedVars),
		Codes:        make(map[string]classifySeedCode),
	}
	if varBatch != nil {
		for scid, meta := range varBatch.Classes {
			cache.Classes[scid] = cloneClassMeta(meta)
		}
		for k, vars := range varBatch.Variables {
			cache.Variables[k.Scid] = classifySeedVars{Height: k.Height, Vars: cloneSCIDVariables(vars)}
		}
	}
	if codeBatch != nil {
		for scid, entry := range codeBatch.SCCodes {
			if entry == nil || entry.Code == "" {
				continue
			}
			cache.Codes[scid] = classifySeedCode{Height: entry.InstallHeight, Code: entry.Code}
		}
	}
	return cache
}

func copyClassMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for class, scids := range in {
		out[class] = append([]string(nil), scids...)
	}
	return out
}

func cloneClassMeta(meta *structures.ClassMeta) *structures.ClassMeta {
	if meta == nil {
		return nil
	}
	cp := *meta
	cp.Tags = append([]string(nil), meta.Tags...)
	return &cp
}

func cloneSCIDVariables(vars []*structures.SCIDVariable) []*structures.SCIDVariable {
	if len(vars) == 0 {
		return nil
	}
	out := make([]*structures.SCIDVariable, len(vars))
	for i, v := range vars {
		if v == nil {
			continue
		}
		cp := *v
		out[i] = &cp
	}
	return out
}
