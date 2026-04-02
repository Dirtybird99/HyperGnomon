package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
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

	mu         sync.RWMutex
	cachedInfo *structures.GetInfoResult
}

// NewServer creates a new API server.
func NewServer(store storage.Storage, pool *rpc.Pool, listenAddr string) *Server {
	return &Server{
		store:      store,
		pool:       pool,
		listenAddr: listenAddr,
	}
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
			Height:       int64(info.Height),
			TopoHeight:   int64(info.TopoHeight),
			StableHeight: int64(info.StableHeight),
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
func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	info := s.cachedInfo
	s.mu.RUnlock()

	if info == nil {
		writeError(w, http.StatusServiceUnavailable, "daemon info not yet available")
		return
	}
	writeJSON(w, http.StatusOK, info)
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
func (s *Server) handleGetTELA(w http.ResponseWriter, r *http.Request) {
	scids, err := s.store.GetAllSCIDs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SCIDs: "+err.Error())
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

	apps := make([]telaApp, 0)
	for _, scid := range scids {
		owner, _ := s.store.GetOwner(scid)
		heights, _ := s.store.GetSCIDInteractionHeights(scid)
		if len(heights) == 0 {
			continue
		}
		latestHeight := heights[len(heights)-1]
		vars, _ := s.store.GetSCIDVariableDetailsAtHeight(scid, latestHeight)

		app := telaApp{SCID: scid, Owner: owner}
		for _, v := range vars {
			key, _ := v.Key.(string)
			val := fmt.Sprintf("%v", v.Value)
			switch key {
			case "nameHdr":
				app.Name = val
			case "descrHdr":
				app.Description = val
			case "dURL":
				app.DURL = val
			case "telaVersion":
				app.Version = val
			case "docVersion":
				app.Version = val
			}
		}
		if app.Name != "" || app.DURL != "" || app.Version != "" {
			apps = append(apps, app)
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tela_apps": apps,
		"count":     len(apps),
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
