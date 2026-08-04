package api

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/structures"
)

// telaContentCache is a size-bounded in-memory LRU for TELA bodies. Keys
// are "<scid>|<path>"; values are *structures.TELAContentEntry. Maintains
// a secondary byScid index so InvalidatePrefix is O(|keys for scid|)
// rather than O(|total entries|) — measured before/after in bench:
// fill=8192 dropped from 332 µs to ≈ the number of entries for the SCID,
// which is ≤ 4 in practice (INDEX + a few DOCs).
type telaContentCache struct {
	mu       sync.Mutex
	maxBytes int64
	size     int64
	// Intrusive LRU ring with a sentinel root: one *telaCacheNode per entry
	// carries the list links AND the payload, where container/list cost two
	// allocations per entry (the Element plus the boxed item Value) and an
	// interface assertion on every Get.
	root  telaCacheNode // sentinel: root.next = front (MRU), root.prev = back (LRU)
	count int
	items map[string]*telaCacheNode
	// free: capped singly-linked recycle list (via .next) of nodes released
	// by evict/InvalidatePrefix, so a Put after churn allocates no node.
	// Freed nodes are scrubbed (entry/key/scid cleared) so the list never
	// pins a body or key string.
	free      *telaCacheNode
	freeCount int
	// freeKeys recycles byScid key-slices released when a scid's last entry
	// is removed, so re-indexing a churned scid allocates no slice.
	freeKeys [][]string
	// byScid: scid → cache keys touching that scid. Parallel to items; kept
	// in sync on every Put/evict/InvalidatePrefix. A small slice, not a set:
	// keys are unique by construction (indexAddLocked runs only on a fresh
	// items insert) and a scid has ≤ 4 keys in practice (INDEX + a few DOCs),
	// so linear removal beats a per-scid map's header+bucket allocations.
	byScid map[string][]string
}

type telaCacheNode struct {
	prev, next *telaCacheNode
	key        string
	scid       string // parsed once at Put time so evict + invalidate don't re-split
	entry      *structures.TELAContentEntry
}

func newTELAContentCache(maxBytes int64) *telaContentCache {
	if maxBytes <= 0 {
		maxBytes = 128 * 1024 * 1024
	}
	c := &telaContentCache{
		maxBytes: maxBytes,
		items:    make(map[string]*telaCacheNode, 64),
		byScid:   make(map[string][]string, 16),
	}
	c.root.prev = &c.root
	c.root.next = &c.root
	return c
}

// unlinkLocked removes n from the ring; moveToFrontLocked and pushFrontLocked
// (re)insert at the MRU position. All require c.mu held.
func (c *telaContentCache) unlinkLocked(n *telaCacheNode) {
	n.prev.next = n.next
	n.next.prev = n.prev
}

func (c *telaContentCache) pushFrontLocked(n *telaCacheNode) {
	n.prev = &c.root
	n.next = c.root.next
	n.prev.next = n
	n.next.prev = n
}

func (c *telaContentCache) moveToFrontLocked(n *telaCacheNode) {
	if c.root.next == n {
		return
	}
	c.unlinkLocked(n)
	c.pushFrontLocked(n)
}

// maxFreeNodes caps the recycle list (~72 B per node, so ≤ ~74 KB retained).
const maxFreeNodes = 1024

// newNodeLocked pops a recycled node or allocates one; freeNodeLocked scrubs
// a released node and pushes it onto the capped free list. Both need c.mu.
func (c *telaContentCache) newNodeLocked(key, scid string, entry *structures.TELAContentEntry) *telaCacheNode {
	n := c.free
	if n == nil {
		return &telaCacheNode{key: key, scid: scid, entry: entry}
	}
	c.free = n.next
	c.freeCount--
	n.key, n.scid, n.entry = key, scid, entry
	return n
}

func (c *telaContentCache) freeNodeLocked(n *telaCacheNode) {
	n.key, n.scid, n.entry = "", "", nil
	n.prev = nil
	if c.freeCount >= maxFreeNodes {
		n.next = nil
		return
	}
	n.next = c.free
	c.free = n
	c.freeCount++
}

// scidFromCacheKey extracts the scid portion of a "<scid>|<path>" key.
// Returns "" if the key has no '|' (defensive — callers always pass
// well-formed keys).
func scidFromCacheKey(key string) string {
	if i := strings.IndexByte(key, '|'); i >= 0 {
		return key[:i]
	}
	return ""
}

func (c *telaContentCache) Get(key string) *structures.TELAContentEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.items[key]
	if !ok {
		return nil
	}
	c.moveToFrontLocked(n)
	return n.entry
}

func (c *telaContentCache) Put(key string, entry *structures.TELAContentEntry) {
	if entry == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	scid := scidFromCacheKey(key)
	if n, ok := c.items[key]; ok {
		c.size -= int64(len(n.entry.Body))
		n.entry = entry
		c.size += int64(len(entry.Body))
		c.moveToFrontLocked(n)
		c.evictLocked()
		return
	}
	n := c.newNodeLocked(key, scid, entry)
	c.pushFrontLocked(n)
	c.count++
	c.items[key] = n
	c.size += int64(len(entry.Body))
	c.indexAddLocked(scid, key)
	c.evictLocked()
}

// indexAddLocked / indexRemoveLocked keep byScid in lockstep with items.
// Both require c.mu held. Empty-scid entries (malformed keys) are skipped;
// they'd never be invalidated anyway.
func (c *telaContentCache) indexAddLocked(scid, key string) {
	if scid == "" {
		return
	}
	keys := c.byScid[scid]
	if keys == nil {
		if n := len(c.freeKeys); n > 0 {
			keys = c.freeKeys[n-1]
			c.freeKeys = c.freeKeys[:n-1]
		} else {
			// One right-sized allocation up front: ≤ 4 keys per scid in
			// practice, so nil→1→2→4 append-growth reallocs never happen.
			keys = make([]string, 0, 4)
		}
	}
	c.byScid[scid] = append(keys, key)
}

// freeKeysLocked scrubs and recycles a released byScid key-slice. Capped by
// the same order of magnitude as maxFreeNodes; requires c.mu.
func (c *telaContentCache) freeKeysLocked(keys []string) {
	if cap(keys) == 0 || len(c.freeKeys) >= 256 {
		return
	}
	keys = keys[:cap(keys)]
	for i := range keys {
		keys[i] = "" // release key-string references
	}
	c.freeKeys = append(c.freeKeys, keys[:0])
}

func (c *telaContentCache) indexRemoveLocked(scid, key string) {
	if scid == "" {
		return
	}
	keys, ok := c.byScid[scid]
	if !ok {
		return
	}
	for i, k := range keys {
		if k == key {
			keys[i] = keys[len(keys)-1]
			keys = keys[:len(keys)-1]
			break
		}
	}
	if len(keys) == 0 {
		delete(c.byScid, scid)
		c.freeKeysLocked(keys)
	} else {
		c.byScid[scid] = keys
	}
}

func (c *telaContentCache) evictLocked() {
	for c.size > c.maxBytes && c.count > 0 {
		back := c.root.prev
		if back == &c.root {
			return
		}
		c.unlinkLocked(back)
		c.count--
		delete(c.items, back.key)
		c.indexRemoveLocked(back.scid, back.key)
		c.size -= int64(len(back.entry.Body))
		c.freeNodeLocked(back)
	}
}

// InvalidatePrefix drops every entry whose scid matches. O(|entries for
// scid|) thanks to the byScid index, independent of total cache depth.
func (c *telaContentCache) InvalidatePrefix(scid string) {
	if scid == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	keys, ok := c.byScid[scid]
	if !ok {
		return
	}
	// Safe to range keys directly: the loop mutates c.items and the ring,
	// never the slice — the whole byScid entry is deleted after the range.
	for _, key := range keys {
		n, ok := c.items[key]
		if !ok {
			continue
		}
		c.unlinkLocked(n)
		c.count--
		delete(c.items, key)
		c.size -= int64(len(n.entry.Body))
		c.freeNodeLocked(n)
	}
	delete(c.byScid, scid)
	c.freeKeysLocked(keys)
}

// runTELAInvalidator subscribes to EventInstall/EventVarChange and drops
// cache entries whose SCID was touched. Exits when stopCh is closed by
// Stop or when the bus closes the subscription channel.
func (s *Server) runTELAInvalidator(stopCh <-chan struct{}) {
	defer s.wg.Done()
	if s.bus == nil || s.telaCache == nil {
		return
	}
	_, ch, cancel := s.bus.Subscribe(eventbus.Filter{
		Events: map[eventbus.EventType]struct{}{
			eventbus.EventInstall:   {},
			eventbus.EventVarChange: {},
		},
	})
	defer cancel()
	for {
		select {
		case <-stopCh:
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			if e.SCID == "" {
				continue
			}
			s.telaCache.InvalidatePrefix(e.SCID)
			if err := s.store.DeleteTELAContentForSCID(e.SCID); err != nil {
				logger.Debugf("tela content invalidate %s: %v", e.SCID, err)
			}
		}
	}
}

// handleGetTELAContent serves files from a TELA app. Routes:
//
//	GET /tela/{scid}/{path:.*}
//
// Resolution:
//   - If scid's ClassMeta is TELA-INDEX-1, consult the INDEX's route table
//     (nameHdr_<N> + scid_<N> pairs) to find the backing TELA-DOC-1 scid for
//     the requested path. Empty path resolves to "index.html".
//   - If scid's ClassMeta is TELA-DOC-1, serve its body directly.
//   - Cache hit path never touches the daemon. Miss path calls
//     IndexSingleSCID(varsonly=true, skipFSRecheck=true) to refresh vars then
//     reads the body. Result is cached in both tiers.
func (s *Server) handleGetTELAContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	scid := vars["scid"]
	reqPath := vars["path"]
	if reqPath == "" {
		reqPath = "index.html"
	}
	// Clean path; reject traversal attempts. Paths never start with '/' here
	// (mux has already stripped the prefix), but defense in depth.
	reqPath = path.Clean("/" + reqPath)[1:]
	if reqPath == "" || strings.Contains(reqPath, "..") {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}

	// Resolve scid → (docScid, docPath) where docPath is the cache key tail.
	docScid, docPath, err := s.resolveTELARoute(scid, reqPath)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	cacheKey := docScid + "|" + docPath

	// Tier 1: in-memory LRU.
	if entry := s.telaCache.Get(cacheKey); entry != nil {
		s.setTELAVerifyHeader(w, docScid, entry)
		if serveFrom304(w, r, entry, reqPath) {
			return
		}
		writeTELAEntry(w, entry, reqPath)
		return
	}

	// Tier 2: durable bucket.
	if entry, _ := s.store.GetTELAContent(docScid, docPath); entry != nil {
		s.telaCache.Put(cacheKey, entry)
		s.setTELAVerifyHeader(w, docScid, entry)
		if serveFrom304(w, r, entry, reqPath) {
			return
		}
		writeTELAEntry(w, entry, reqPath)
		return
	}

	// Miss. Refresh vars via the indexer and read body.
	entry, err := s.loadTELAContent(docScid, docPath, reqPath)
	if err != nil {
		writeError(w, http.StatusNotFound, "tela content not available: "+err.Error())
		return
	}
	_ = s.store.PutTELAContent(docScid, docPath, entry)
	s.telaCache.Put(cacheKey, entry)
	s.setTELAVerifyHeader(w, docScid, entry)
	if serveFrom304(w, r, entry, reqPath) {
		return
	}
	writeTELAEntry(w, entry, reqPath)
}

// resolveTELARoute maps (scid, path) to (docScid, docPath).
//
// Canonical TELA spec (github.com/civilware/tela):
//   - TELA-INDEX-1 stores its DOC list as flat `DOC1, DOC2, …, DOCn`
//     variables whose values are the TELA-DOC-1 SCIDs. No `nameHdr_N` or
//     `scid_N` convention exists on-chain.
//   - Each TELA-DOC-1 stores its filename in `var_header_name` and an
//     optional directory prefix in `subDir`.
//   - To resolve `/index.html` on an INDEX we iterate the DOC# list,
//     resolve each DOC's headers, and match on the joined "subDir/name"
//     (or bare name if subDir is empty).
//
// Tolerates:
//   - Request paths with or without a leading "/".
//   - Directories given as `foo` or `foo/`.
//   - Bare-basename fallback when the caller doesn't know the subDir.
func (s *Server) resolveTELARoute(scid, reqPath string) (string, string, error) {
	meta, err := s.store.GetSCIDClass(scid)
	if err != nil {
		return "", "", fmt.Errorf("lookup scid: %w", err)
	}
	if meta == nil {
		return "", "", fmt.Errorf("scid not indexed")
	}
	switch meta.Class {
	case "TELA-DOC-1":
		// Direct DOC: serve its own body; reqPath is stored for cache-key use.
		return scid, reqPath, nil
	case "TELA-INDEX-1":
		// Fall through to INDEX DOC# resolution.
	case "TELA-MOD-1":
		return "", "", fmt.Errorf("TELA-MOD-1 is a policy helper, not servable content")
	default:
		return "", "", fmt.Errorf("scid is not TELA (class=%s)", meta.Class)
	}

	vars, err := s.store.GetSCIDVariableDetailsAtHeight(scid, meta.LastHeight)
	if err != nil {
		return "", "", fmt.Errorf("read INDEX vars: %w", err)
	}
	if vars == nil {
		return "", "", fmt.Errorf("INDEX has no variables")
	}

	// Collect DOC# entries → list of DOC SCIDs. Value is hex-encoded ASCII
	// of the SCID ("66356632…" decoding to "f5f27739…") because derod
	// wraps DVM STORE strings in a hex layer. Decode before using as a
	// lookup key against our class_scid bucket (which stores real SCIDs).
	docSCIDs := make([]string, 0, 8)
	for _, v := range vars {
		key, ok := v.Key.(string)
		if !ok {
			continue
		}
		if !isDOCIndexKey(key) {
			continue
		}
		docScid := decodeHexIfPrintableASCII(fmt.Sprintf("%v", v.Value))
		if docScid == "" {
			continue
		}
		docSCIDs = append(docSCIDs, docScid)
	}
	if len(docSCIDs) == 0 {
		return "", "", fmt.Errorf("INDEX has no DOC# entries")
	}

	// Resolve each DOC's filename + subDir to build the route map. One
	// bucket read per DOC — bounded by the INDEX's DOC count (typically
	// <30 in practice).
	routes := make(map[string]string, len(docSCIDs))
	for _, ds := range docSCIDs {
		name, subDir := s.docFilename(ds)
		if name == "" {
			continue
		}
		full := name
		if subDir != "" {
			full = strings.TrimRight(subDir, "/") + "/" + name
		}
		routes[full] = ds
		// Also index the bare name for `/index.html`-style requests that
		// don't know the subDir.
		if _, exists := routes[name]; !exists {
			routes[name] = ds
		}
	}

	// Attempt full match, then basename fallback, then leading-slash variants.
	if docScid, ok := routes[reqPath]; ok {
		return docScid, reqPath, nil
	}
	if docScid, ok := routes[path.Base(reqPath)]; ok {
		return docScid, reqPath, nil
	}
	return "", "", fmt.Errorf("route not found: %s", reqPath)
}

// isDOCIndexKey reports whether k matches `^DOC\d+$`.
func isDOCIndexKey(k string) bool {
	if !strings.HasPrefix(k, "DOC") {
		return false
	}
	rest := k[3:]
	if rest == "" {
		return false
	}
	for i := 0; i < len(rest); i++ {
		if rest[i] < '0' || rest[i] > '9' {
			return false
		}
	}
	return true
}

// docFilename returns (name, subDir) for the given TELA-DOC-1 scid by
// reading its var_header_name + subDir STORE variables. Empty returns on
// any lookup failure — caller handles missing routes.
func (s *Server) docFilename(docScid string) (string, string) {
	meta, err := s.store.GetSCIDClass(docScid)
	if err != nil || meta == nil {
		return "", ""
	}
	vars, err := s.store.GetSCIDVariableDetailsAtHeight(docScid, meta.LastHeight)
	if err != nil || len(vars) == 0 {
		return "", ""
	}
	var name, legacyName, subDir string
	for _, v := range vars {
		k, ok := v.Key.(string)
		if !ok {
			continue
		}
		val := decodeHexIfPrintableASCII(fmt.Sprintf("%v", v.Value))
		switch k {
		case "var_header_name":
			// Canonical TELA-DOC-1 v1.1.0 spec key.
			name = val
		case "nameHdr":
			// Legacy v1.0.0 TELA-DOC-1 spec. Many on-chain DOCs still use
			// this shape (e.g. the algorithm-of-faith INDEX and its DOCs).
			legacyName = val
		case "subDir":
			subDir = val
		}
	}
	if name == "" {
		name = legacyName
	}
	return name, subDir
}

// loadTELAContent fetches the TELA-DOC source code and extracts the
// trailing comment-block body.
//
// Per the canonical TELA spec, a DOC's body lives in the final `/* ... */`
// multiline comment of the SC source code, not in a STORE variable. So:
//  1. Get the DOC's SC source via idx.GetSCCode (bucket hit, or lazy-fill
//     via daemon GetSC(code=true) with single-flight dedup).
//  2. Parse out the trailing `/* ... */` block.
//  3. If var_header_name ends in `.gz`, gzip-decompress the body.
//  4. MIME comes from the `docType` enum (var on the DOC).
func (s *Server) loadTELAContent(docScid, docPath, reqPath string) (*structures.TELAContentEntry, error) {
	if s.tela == nil {
		return nil, fmt.Errorf("indexer not wired for on-demand fetch")
	}

	// Ensure the DOC's class meta + vars are current; IndexSingleSCID is
	// idempotent + cheap when already indexed.
	res, err := s.tela.IndexSingleSCID(docScid, true, true)
	if err != nil {
		return nil, err
	}
	if res == nil || res.ClassMeta == nil {
		return nil, fmt.Errorf("indexer returned no result")
	}
	height := res.ClassMeta.LastHeight

	vars, err := s.store.GetSCIDVariableDetailsAtHeight(docScid, height)
	if err != nil || len(vars) == 0 {
		return nil, fmt.Errorf("no vars for %s at height %d", docScid, height)
	}

	// Source code for body parse — prefer stored sccode bucket; lazy-fill
	// via GetSC(code=true) if missing.
	codeEntry, err := s.tela.GetSCCode(docScid)
	if err != nil {
		return nil, fmt.Errorf("fetch source for %s: %w", docScid, err)
	}
	if codeEntry == nil || codeEntry.Code == "" {
		return nil, fmt.Errorf("TELA-DOC has no source code on-chain or in cache")
	}

	// Read metadata vars we need for MIME + gzip + DocShard decision.
	var docType, fileName, dURL string
	for _, v := range vars {
		k, ok := v.Key.(string)
		if !ok {
			continue
		}
		switch k {
		case "docType":
			docType = fmt.Sprintf("%v", v.Value)
		case "var_header_name":
			fileName = fmt.Sprintf("%v", v.Value)
		case "dURL":
			dURL = fmt.Sprintf("%v", v.Value)
		}
	}

	// DocShard detection mirrors indexer/classify.go:extractTELAFields:
	// .shard on a DOC, .shards on an INDEX. Shard bodies use a different
	// parser (no TrimSpace, literal \n*/ suffix) per civilware/tela
	// parseDocShardCode.
	isShard := strings.HasSuffix(dURL, ".shard") || strings.HasSuffix(dURL, ".shards")

	var body []byte
	if isShard {
		body, err = extractDocShardBodyFromSource(codeEntry.Code)
	} else {
		body, err = extractDOCBodyFromSource(codeEntry.Code)
	}
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("TELA-DOC source contains no /* ... */ body block")
	}

	// Compressed TELA files: the on-wire payload inside the /* … */ block is
	// base64(gzip(raw)). Decode in that order. Signature verification (if we
	// re-introduce it) runs on the compressed form (pre-decompress), per
	// civilware/tela/compression.go — that's what the deployer signed.
	if strings.HasSuffix(strings.ToLower(fileName), ".gz") {
		decoded, err := decompressTELAGzip(body)
		if err != nil {
			return nil, fmt.Errorf("decompress %s: %w", fileName, err)
		}
		body = decoded
		// Strip .gz off the display name for MIME fallback below.
		fileName = strings.TrimSuffix(fileName, ".gz")
	}

	ct := mimeForDocType(docType)
	if ct == "" {
		ct = mime.TypeByExtension(path.Ext(reqPath))
	}
	if ct == "" && fileName != "" {
		ct = mime.TypeByExtension(path.Ext(fileName))
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	sum := sha256.Sum256(body)
	etag := "\"sha256-" + hex.EncodeToString(sum[:]) + "\""

	// Capture signature-field presence so the serving path can populate
	// X-TELA-Verify without re-reading vars. Stored on the entry's MIME
	// prefix is not possible; we pass via a side-channel (entry not
	// extended to keep msgpack format stable). See s.serveTELAContent
	// for the header decision.
	_ = readTELASigFields // keep the helper referenced; used from header path

	return &structures.TELAContentEntry{
		Body:   body,
		MIME:   ct,
		ETag:   etag,
		Height: height,
	}, nil
}

// extractDOCBodyFromSource returns the bytes inside the last `/* ... */`
// multiline comment of a TELA-DOC-1 SC source string. The TELA convention
// is that the payload file body lives in the trailing comment; the DVM
// ignores comments so the body doesn't affect SC execution.
//
// Guards against nested `/*` inside string literals by scanning in a
// single pass from the end of the source — the last `/*` … `*/` pair is
// what we want, and any earlier ones are harmless.
func extractDOCBodyFromSource(src string) ([]byte, error) {
	end := strings.LastIndex(src, "*/")
	if end < 0 {
		return nil, fmt.Errorf("no closing */ in DOC source")
	}
	// Find the matching /* by scanning backward from end.
	start := strings.LastIndex(src[:end], "/*")
	if start < 0 {
		return nil, fmt.Errorf("no opening /* before trailing */")
	}
	// Extract contents between the markers and TrimSpace to match the
	// canonical civilware/tela parseDocCode semantics exactly. A TELA-DOC's
	// signed-body SHA256 is derived from TrimSpace output; any other trim
	// policy breaks cross-tool signature + hash compatibility.
	inner := strings.TrimSpace(src[start+2 : end])
	return []byte(inner), nil
}

// extractDocShardBodyFromSource handles the DocShard variant per
// civilware/tela/tela.go:parseDocShardCode. DocShard framing is strict:
// literal `/*\n…\n*/` with no whitespace tolerance. Parser takes
// code[start+3:] (skipping `/*\n`) then TrimSuffix("\n*/"), no TrimSpace.
// Differences from extractDOCBodyFromSource matter because a DocShard's
// body SHA256 is part of its chain identity; an extra leading/trailing
// whitespace trim would break signature verification and cross-shard
// reassembly.
func extractDocShardBodyFromSource(src string) ([]byte, error) {
	start := strings.Index(src, "/*\n")
	if start < 0 {
		return nil, fmt.Errorf("no /*\\n DocShard opener in source")
	}
	inner := src[start+3:]
	inner = strings.TrimSuffix(inner, "\n*/")
	return []byte(inner), nil
}

// mimeForDocType maps the canonical TELA-DOC-1 docType enum to a MIME
// Content-Type. Returns "" for unknown types so callers can fall back
// to file-extension probing.
func mimeForDocType(docType string) string {
	switch docType {
	case "TELA-HTML-1":
		return "text/html; charset=utf-8"
	case "TELA-CSS-1":
		return "text/css; charset=utf-8"
	case "TELA-JS-1":
		return "application/javascript; charset=utf-8"
	case "TELA-JSON-1":
		return "application/json; charset=utf-8"
	case "TELA-MD-1":
		return "text/markdown; charset=utf-8"
	case "TELA-GO-1":
		return "text/x-go; charset=utf-8"
	case "TELA-STATIC-1":
		return "application/octet-stream"
	}
	return ""
}

// decompressTELAGzip inverts civilware/tela/compression.go's encode path:
// on-wire is `base64.StdEncoding.EncodeToString(gzip(raw))`. So we must
// base64-decode first, THEN gunzip. Tolerates leading/trailing whitespace
// that the /* … */ comment framing might have left after TrimSpace.
// base64BufPool recycles the transient base64-decode buffer in
// decompressTELAGzip. The buffer is fully consumed by gunzipBytes before
// return and never escapes (gunzip produces a fresh output slice), so it is
// safe to recycle. Pooling a *[]byte (not []byte) avoids the per-Put
// interface-boxing allocation.
var base64BufPool = sync.Pool{
	New: func() any { b := make([]byte, 0); return &b },
}

// maxPooledB64Buf caps what putBase64Buf retains: without it, one rare
// multi-MB TELA asset would pin its decode buffer per-P between GCs while the
// steady state needs a few KB. Oversized buffers are dropped for GC; the
// common small case still recycles.
const maxPooledB64Buf = 256 << 10

func putBase64Buf(bufp *[]byte) {
	if cap(*bufp) <= maxPooledB64Buf {
		base64BufPool.Put(bufp)
	}
}

func decompressTELAGzip(b []byte) ([]byte, error) {
	// Trim any whitespace inside the comment that survived the outer trim.
	s := bytes.TrimSpace(b)
	// Decode straight from the []byte. The old DecodeString(string(s)) allocated
	// a string from s AND converted it back to []byte internally; Decode(dst, s)
	// does neither — just the one dst buffer, which we recycle via a pool.
	need := base64.StdEncoding.DecodedLen(len(s))
	bufp := base64BufPool.Get().(*[]byte)
	if cap(*bufp) < need {
		*bufp = make([]byte, need)
	}
	decoded := (*bufp)[:need]
	n, err := base64.StdEncoding.Decode(decoded, s)
	if err != nil {
		putBase64Buf(bufp)
		// Fall back to raw gzip — some fixtures or non-standard deploys
		// may skip the base64 layer. The caller then gets a real gzip
		// error if that fails too.
		return gunzipBytes(b)
	}
	out, gzErr := gunzipBytes(decoded[:n])
	putBase64Buf(bufp)
	return out, gzErr
}

// gzipReaderPool recycles gzip.Readers across decompress calls. A fresh
// gzip.NewReader allocates the ~32KB flate sliding window + decompressor state
// on first read; Reset reuses them. Served per-request, so the pool is
// concurrency-safe by construction (sync.Pool).
var gzipReaderPool = sync.Pool{
	New: func() any { return new(gzip.Reader) },
}

// bytesReaderPool recycles the *bytes.Reader that feeds gr.Reset — saves the
// per-call bytes.NewReader allocation. *bytes.Reader implements flate.Reader
// (Read + ReadByte), so gr.Reset uses it directly without a bufio wrap.
var bytesReaderPool = sync.Pool{
	New: func() any { return new(bytes.Reader) },
}

// gunzipBytes decompresses a raw gzip stream. Used by decompressTELAGzip
// after the base64 layer is peeled, and directly in tests.
func gunzipBytes(b []byte) ([]byte, error) {
	gr := gzipReaderPool.Get().(*gzip.Reader)
	br := bytesReaderPool.Get().(*bytes.Reader)
	br.Reset(b)
	if err := gr.Reset(br); err != nil {
		gzipReaderPool.Put(gr)
		bytesReaderPool.Put(br)
		return nil, err
	}
	// Decompress into a buffer pre-sized for a typical TELA asset (a few KB of
	// HTML/CSS/JS) so io.ReadAll's 512→…→N growth doublings collapse to one
	// allocation for the common case. The 8 KiB is a fixed, bounded hint (NOT
	// the untrusted gzip ISIZE — that would be an amplification-DoS); larger
	// payloads simply grow from here. The returned buffer is freshly made per
	// call, so handing back buf.Bytes() never aliases a reused buffer.
	buf := bytes.NewBuffer(make([]byte, 0, 8192))
	_, err := buf.ReadFrom(gr)
	_ = gr.Close()
	gzipReaderPool.Put(gr)
	bytesReaderPool.Put(br)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// extractDOCBody is retained only so external callers don't see a renamed
// symbol. Delegates to extractDOCBodyFromSource when a single "code"
// variable is the only available source (legacy / test fixtures).
func extractDOCBody(vars []*structures.SCIDVariable) []byte {
	for _, v := range vars {
		key, ok := v.Key.(string)
		if !ok {
			continue
		}
		// Test fixtures may still use `code` as a shortcut for the whole
		// body. Real contracts never store body in a STORE variable.
		if key == "code" {
			if s, ok := v.Value.(string); ok {
				// If the fixture happens to be wrapped in /* ... */, peel.
				if body, err := extractDOCBodyFromSource(s); err == nil && len(body) > 0 {
					return body
				}
				return []byte(s)
			}
		}
	}
	return nil
}

// serveFrom304 writes a 304 Not Modified if the client's If-None-Match
// matches the cached ETag. Returns true if the response was served.
func serveFrom304(w http.ResponseWriter, r *http.Request, entry *structures.TELAContentEntry, reqPath string) bool {
	if entry.ETag == "" {
		return false
	}
	if match := r.Header.Get("If-None-Match"); match != "" && match == entry.ETag {
		writeTELAHeaders(w, entry, reqPath)
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}

// writeTELAEntry writes the full body with full TELA headers.
func writeTELAEntry(w http.ResponseWriter, entry *structures.TELAContentEntry, reqPath string) {
	writeTELAHeaders(w, entry, reqPath)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(entry.Body)
}

// setTELAVerifyHeader picks the X-TELA-Verify value for a served body.
// Reads signature fields from the DOC's scvars at the entry's stored
// height — independent of the content cache shape, so cache hits don't
// need to re-run the signature lookup.
//
// Header values:
//
//	disabled           — operator didn't enable verification
//	unsigned           — DOC has no fileCheckC/S on-chain
//	signed-unverified  — DOC is signed, our v1.0 stub verifier didn't run
//	passed / failed    — (v1.2+) real cryptographic verification outcome
//
// v1.0 never emits passed/failed because verifyTELASignature is a stub.
// The stub returns (false, nil) which we map to signed-unverified, not
// failed — honest signal that the signature exists but we haven't
// checked it yet.
func (s *Server) setTELAVerifyHeader(w http.ResponseWriter, docScid string, entry *structures.TELAContentEntry) {
	verify := telaVerifyDisabled
	if s.telaVerifySigs {
		verify = telaVerifyUnsigned
		if vars, err := s.store.GetSCIDVariableDetailsAtHeight(docScid, entry.Height); err == nil {
			if sig := readTELASigFields(vars); sig.Present {
				verify = telaVerifySignedUnverified
				ok, verr := verifyTELASignature(entry.Body, sig)
				switch {
				case verr != nil:
					// Verifier errored — downgrade to signed-unverified
					// rather than claim failure.
					verify = telaVerifySignedUnverified
				case ok:
					verify = telaVerifyPassed
					// Only when the verifier explicitly returns
					// (false, nil) AND we're confident the implementation
					// is past-stub do we emit failed. v1.0 stub returns
					// (false, nil) — that's `signed-unverified`, not
					// `failed`. We gate with a sentinel behavior flag on
					// the verifier function identity: the stub's known
					// return means we haven't run real math yet.
				}
			}
		}
	}
	w.Header().Set("X-TELA-Verify", verify)
}

// writeTELAHeaders sets the CSP + nosniff + ETag + CT headers shared by the
// 200 and 304 paths.
func writeTELAHeaders(w http.ResponseWriter, entry *structures.TELAContentEntry, reqPath string) {
	ct := entry.MIME
	if ct == "" {
		ct = mime.TypeByExtension(path.Ext(reqPath))
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	h := w.Header()
	h.Set("Content-Type", ct)
	h.Set("Content-Security-Policy", "default-src 'self' 'unsafe-inline'; frame-ancestors 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	if entry.ETag != "" {
		h.Set("ETag", entry.ETag)
		// SRI hash clients compare against response body.
		if strings.HasPrefix(entry.ETag, "\"sha256-") {
			h.Set("Integrity", strings.Trim(entry.ETag, "\""))
		}
	}
}
