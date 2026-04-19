package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// Server is the HTTP REST API server for HyperGnomon.
type Server struct {
	store      storage.Storage
	pool       *rpc.Pool
	listenAddr string

	// safeHeight is a pointer to the indexer's atomic.Int64 tracking
	// max(LastIndexedHeight - FinalityDepth, 0). A pointer keeps the api
	// package free of an indexer import while still giving clients a live
	// read. nil is tolerated (returns 0) so callers that don't care can
	// skip wiring it up.
	safeHeight *atomic.Int64

	mu         sync.RWMutex
	cachedInfo *structures.GetInfoResult
}

// NewServer creates a new API server.
//
// safeHeight may be nil; handlers treat nil as zero. Passing a pointer to
// indexer.Indexer.SafeHeight lets the API expose live safe-height reads
// without the api package importing indexer.
func NewServer(store storage.Storage, pool *rpc.Pool, listenAddr string, safeHeight *atomic.Int64) *Server {
	return &Server{
		store:      store,
		pool:       pool,
		listenAddr: listenAddr,
		safeHeight: safeHeight,
	}
}

// loadSafeHeight returns the current safe height, or 0 if not wired.
func (s *Server) loadSafeHeight() int64 {
	if s.safeHeight == nil {
		return 0
	}
	return s.safeHeight.Load()
}

// Start registers routes and begins serving HTTP requests.
// Blocks until the server exits.
func (s *Server) Start() error {
	r := mux.NewRouter()

	r.HandleFunc("/api/getinfo", s.handleGetInfo).Methods(http.MethodGet)
	r.HandleFunc("/api/getstats", s.handleGetStats).Methods(http.MethodGet)
	r.HandleFunc("/api/getscids", s.handleGetSCIDs).Methods(http.MethodGet)
	r.HandleFunc("/api/indexedscs", s.handleIndexedSCs).Methods(http.MethodGet)
	r.HandleFunc("/api/indexbyscid", s.handleIndexBySCID).Methods(http.MethodGet)
	r.HandleFunc("/api/scvarsbyheight", s.handleSCVarsByHeight).Methods(http.MethodGet)
	r.HandleFunc("/api/invalidscids", s.handleInvalidSCIDs).Methods(http.MethodGet)
	r.HandleFunc("/api/scidprivtx", s.handleSCIDPrivTx).Methods(http.MethodGet)
	r.HandleFunc("/api/tela", s.handleGetTELA).Methods(http.MethodGet)
	r.HandleFunc("/api/tela/count", s.handleGetTELACount).Methods(http.MethodGet)
	r.HandleFunc("/api/address/{address}/scs", s.handleGetAddress).Methods(http.MethodGet)

	// Start background info caching
	go s.refreshInfoLoop()

	logger.Infof("HTTP API listening on %s", s.listenAddr)
	srv := &http.Server{
		Addr:         s.listenAddr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return srv.ListenAndServe()
}

// refreshInfoLoop periodically fetches daemon info and caches it.
func (s *Server) refreshInfoLoop() {
	s.refreshInfo()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.refreshInfo()
	}
}

func (s *Server) refreshInfo() {
	err := s.pool.WithConn(func(c *rpc.Client) error {
		info, err := c.GetInfo()
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.cachedInfo = &structures.GetInfoResult{
			Height:       info.Height,
			TopoHeight:   info.TopoHeight,
			StableHeight: info.StableHeight,
			Status:       info.Status,
		}
		s.mu.Unlock()
		return nil
	})
	if err != nil {
		logger.Warnf("refresh daemon info: %v", err)
	}
}

// --- Handlers ---

// handleGetInfo returns cached daemon info.
//
// The response is a struct-like map rather than the bare GetInfoResult so we
// can tack on indexer-specific fields (safe_height) without changing the
// shared cache type. Existing field names are preserved verbatim for back-
// compat with callers that learned them from pre-M1 responses.
func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	info := s.cachedInfo
	s.mu.RUnlock()

	if info == nil {
		writeError(w, http.StatusServiceUnavailable, "daemon info not yet available")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Height":       info.Height,
		"TopoHeight":   info.TopoHeight,
		"StableHeight": info.StableHeight,
		"Status":       info.Status,
		"safe_height":  s.loadSafeHeight(),
	})
}

// handleGetStats returns indexer statistics.
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	indexHeight, err := s.store.GetLastIndexHeight()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get index height: "+err.Error())
		return
	}

	scids, err := s.store.GetAllSCIDs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SCIDs: "+err.Error())
		return
	}

	reg, burn, norm, err := s.store.GetTxCounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get tx counts: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"app_name":       structures.AppName,
		"version":        structures.Version,
		"index_height":   indexHeight,
		"safe_height":    s.loadSafeHeight(),
		"sc_count":       len(scids),
		"reg_tx_count":   reg,
		"burn_tx_count":  burn,
		"norm_tx_count":  norm,
		"total_tx_count": reg + burn + norm,
		"tela_count":     structures.TELACount.Load(),
	})
}

// handleGetSCIDs returns all indexed SCIDs.
func (s *Server) handleGetSCIDs(w http.ResponseWriter, r *http.Request) {
	scids, err := s.store.GetAllSCIDs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SCIDs: "+err.Error())
		return
	}
	if scids == nil {
		scids = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scids": scids,
	})
}

// handleIndexedSCs returns all SCIDs with their owners.
func (s *Server) handleIndexedSCs(w http.ResponseWriter, r *http.Request) {
	owners, err := s.store.GetAllOwnersAndSCIDs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get owners: "+err.Error())
		return
	}
	if owners == nil {
		owners = make(map[string]string)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"indexed_scs": owners,
	})
}

// handleIndexBySCID returns invocation details for a given SCID.
func (s *Server) handleIndexBySCID(w http.ResponseWriter, r *http.Request) {
	scid := r.URL.Query().Get("scid")
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: scid")
		return
	}

	details, err := s.store.GetInvokeDetailsBySCID(scid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get invoke details: "+err.Error())
		return
	}
	if details == nil {
		details = []*structures.SCTXParse{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scid":    scid,
		"details": details,
	})
}

// handleSCVarsByHeight returns SC variables at a specific height.
func (s *Server) handleSCVarsByHeight(w http.ResponseWriter, r *http.Request) {
	scid := r.URL.Query().Get("scid")
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: scid")
		return
	}

	heightStr := r.URL.Query().Get("height")
	if heightStr == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: height")
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid height parameter: "+err.Error())
		return
	}

	vars, err := s.store.GetSCIDVariableDetailsAtHeight(scid, height)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SC variables: "+err.Error())
		return
	}
	if vars == nil {
		vars = []*structures.SCIDVariable{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scid":      scid,
		"height":    height,
		"variables": vars,
	})
}

// handleInvalidSCIDs returns failed SC deploys.
func (s *Server) handleInvalidSCIDs(w http.ResponseWriter, r *http.Request) {
	invalid, err := s.store.GetInvalidSCIDDeploys()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get invalid SCIDs: "+err.Error())
		return
	}
	if invalid == nil {
		invalid = make(map[string]uint64)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"invalid_scids": invalid,
	})
}

// handleSCIDPrivTx returns normal TXs with SCID payload for a given address.
func (s *Server) handleSCIDPrivTx(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("address")
	if addr == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: address")
		return
	}

	txs, err := s.store.GetNormalTxWithSCIDByAddr(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get normal TXs: "+err.Error())
		return
	}
	if txs == nil {
		txs = []*structures.NormalTXWithSCIDParse{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address":      addr,
		"transactions": txs,
	})
}

// handleGetTELA returns all discovered TELA SCIDs with their metadata.
// Uses the class index (Route B) for an O(1) prefix scan instead of the old
// O(N * 3-reads) iteration over every SCID. Accepts an optional ?class= query
// param (defaults to "TELA-INDEX-1"; "TELA-DOC-1" is the other common value).
func (s *Server) handleGetTELA(w http.ResponseWriter, r *http.Request) {
	class := r.URL.Query().Get("class")
	if class == "" {
		class = "TELA-INDEX-1"
	}

	installs, err := s.store.GetClassInstalls(class, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get class installs: "+err.Error())
		return
	}

	type telaApp struct {
		SCID        string `json:"scid"`
		Name        string `json:"name,omitempty"`
		Description string `json:"description,omitempty"`
		DURL        string `json:"durl,omitempty"`
		Version     string `json:"version,omitempty"`
		Owner       string `json:"owner,omitempty"`
	}

	apps := make([]telaApp, 0, len(installs))
	for _, inst := range installs {
		owner, _ := s.store.GetOwner(inst.SCID)
		app := telaApp{
			SCID:  inst.SCID,
			Owner: owner,
		}
		if inst.Meta != nil {
			app.Name = inst.Meta.Name
			app.Description = inst.Meta.Desc
		}
		// DURL and Version are not yet captured on ClassMeta (M0); leave blank.
		apps = append(apps, app)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tela_apps": apps,
		"count":     len(apps),
	})
}

// handleGetAddress returns the list of SCIDs an address has interacted with,
// enriched with class + name. Sorted by last_height descending so the most
// recently-touched SCIDs come first.
func (s *Server) handleGetAddress(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	if addr == "" {
		writeError(w, http.StatusBadRequest, "missing required path parameter: address")
		return
	}

	entries, err := s.store.GetAddressSCIDs(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get address SCIDs: "+err.Error())
		return
	}

	type scidEntry struct {
		SCID        string `json:"scid"`
		FirstHeight int64  `json:"first_height"`
		LastHeight  int64  `json:"last_height"`
		Count       int64  `json:"count"`
		Class       string `json:"class,omitempty"`
		Name        string `json:"name,omitempty"`
	}

	out := make([]scidEntry, 0, len(entries))
	for scid, e := range entries {
		if e == nil {
			continue
		}
		item := scidEntry{
			SCID:        scid,
			FirstHeight: e.FirstHeight,
			LastHeight:  e.LastHeight,
			Count:       e.Count,
		}
		if meta, _ := s.store.GetSCIDClass(scid); meta != nil {
			item.Class = meta.Class
			item.Name = meta.Name
		}
		out = append(out, item)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LastHeight > out[j].LastHeight
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address": addr,
		"scids":   out,
		"count":   len(out),
	})
}

// handleGetTELACount returns the current TELA discovery count from the atomic counter.
// Zero-allocation, no DB queries -- suitable for fast UI polling.
func (s *Server) handleGetTELACount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{
		"tela_count": structures.TELACount.Load(),
	})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Errorf("json encode: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
