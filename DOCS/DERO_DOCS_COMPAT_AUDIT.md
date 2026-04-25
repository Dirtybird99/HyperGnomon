# DHEBP dero-docs Compatibility Audit

Source reviewed: https://github.com/DHEBP/dero-docs at `5d5c3134854fb49927823321041c86513d0a6ff9`.

This audit maps the TELA, Hologram, DeroPay, DeroAuth, and daemon RPC docs to HyperGnomon's current behavior. It is intentionally test-focused: implementation gaps are recorded, but not patched here.

## Current Coverage

### TELA Content Serving

Status: mostly covered.

Validated behavior:

- `TELA-INDEX-1` route resolution uses canonical `DOC1`, `DOC2`, ... variables.
- `TELA-DOC-1` filenames resolve through canonical `var_header_name`.
- Legacy `nameHdr` filenames still resolve.
- `subDir` paths and bare basename fallback both work.
- `TELA-MOD-1` is rejected as policy/helper content, not directly servable.
- Normal DOC body extraction, DocShard strict framing, base64+gzip decode, raw gzip fallback, invalid gzip failure, MIME mapping, and missing body errors are covered by API tests.

Tests:

- `TestDeroDocsCompat_RouteResolution`
- `TestDeroDocsCompat_InvalidGzipFails`
- Existing `TestExtractDOCBodyFromSource`
- Existing `TestExtractDocShardBodyFromSource`
- Existing `TestDecompressTELAGzip`
- Existing `TestMIMEForDocType`

### Discovery, Ratings, and Gnomon-Style APIs

Status: partially covered.

Validated behavior:

- REST `/api/tela` returns class-indexed TELA app metadata.
- REST `/api/tela/count` returns the discovered TELA count.
- REST `/api/tela/{scid}/ratings` reads canonical rating keys and aggregate `likes` / `dislikes`.
- WS `listsc`, `listsc_byclass`, `listsc_variables`, `listsc_ratings`, and `listsc_byowner` return usable envelopes for TELA/Hologram-style consumers.

Tests:

- `TestDeroDocsCompat_HTTPDiscoveryAndRatings`
- `TestDeroDocsCompat_WSListSCFamily`
- Existing storage ratings tests.

Remaining gaps:

- No first-class dURL resolver endpoint with an explicit duplicate-dURL policy.
- No min-likes filtering endpoint matching TELA-CLI discovery workflows.
- No generic metadata key/value search endpoint.
- No code line search endpoint.

### Hologram Offline-First Compatibility

Status: partial.

Useful current behavior:

- TELA content cache entries include body, MIME, ETag, and height.
- ETags are SHA256-derived and support cache validation.
- Ratings and class metadata are available for discovery and quality filtering.
- Owner and class lookups support browser/explorer presentation.

Remaining gaps:

- No offline prefetch API.
- No cached-versus-on-chain diff API.
- No version/update summary API for all cached apps.
- No explicit cache stats or eviction-control API exposed to clients.

### Signature Integrity

Status: known limitation.

Current behavior:

- `fileCheckC` and `fileCheckS` are parsed.
- `X-TELA-Verify` can report signature presence.
- Cryptographic BN256 Schnorr verification is not implemented.

Remaining gap:

- Implement real DERO signature verification using the DeroAuth crypto model: Schnorr over BN256, Keccak-256 reduced modulo curve order, DERO address public-key decoding, and the DERO generator point.

### Daemon RPC and Indexer Assumptions

Status: mostly aligned.

Useful current behavior:

- `GetInfo` exposes height, topoheight, stableheight, status, and HyperGnomon safe height.
- `GetSC` is used with targeted `Code`, `Variables`, and `KeysString` modes.
- TELA INDEX refreshes use full variable fetches because route tables are variable length.
- TELA DOC refreshes use targeted keys for bounded metadata and signature fields.

Remaining gaps:

- Live RPC compatibility should be checked periodically against `203.0.113.10:10102`.
- A small live probe should assert daemon `DERO.GetInfo`, HyperGnomon `/api/getinfo`, `/api/tela`, and WS `listsc_byclass` shapes after startup.

## Test Matrix

Local required checks:

```powershell
go test ./api
go test ./storage
go test ./...
```

Optional live probe checklist against the user's daemon:

```powershell
$body = @{
  jsonrpc = "2.0"
  id = "1"
  method = "DERO.GetInfo"
} | ConvertTo-Json
Invoke-RestMethod -Uri "http://203.0.113.10:10102/json_rpc" -Method Post -ContentType "application/json" -Body $body
```

Latest daemon probe from April 24, 2026:

- Endpoint: `203.0.113.10:10102`
- Method: `DERO.GetInfo`
- Result: `status=OK`, `height=6952012`, `topoheight=6952012`, `stableheight=6952004`
- Daemon version: `3.5.5-142.DEROHE.STARGATE+13082025`

When HyperGnomon is running on a temp DB against that daemon, verify:

- `GET /api/getinfo` returns `Height`, `TopoHeight`, `StableHeight`, and `safe_height`.
- `GET /api/tela` returns at least a stable empty envelope, and real TELA rows after classification completes.
- `GET /api/tela/count` matches the in-process TELA count.
- `GET /api/tela/{scid}/ratings` returns canonical `ratings`, `count`, `avg`, and optional `summary`.
- WS `listsc_byclass` with `{"class":"TELA-INDEX-1"}` returns paginated `results`.

## Recommended Next Implementation Items

1. Add a dURL search/resolve API that returns all matches, not a silent first match.
2. Add discovery filters for min-likes, class, owner, dURL, metadata key/value, and text/code line search.
3. Add offline cache inspection endpoints for cached apps, cache stats, update checks, and diff summaries.
4. Implement real `fileCheckC` / `fileCheckS` verification and change `X-TELA-Verify` from `signed-unverified` to `passed` or `failed` when verification is enabled.
