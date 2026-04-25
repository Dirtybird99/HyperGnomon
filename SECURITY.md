# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in HyperGnomon, please report it privately.

**Do not open a public GitHub issue for security vulnerabilities.**

Use **GitHub Security Advisories**: select "Report a vulnerability" on the Security tab of the repository.

Please include:

- The HyperGnomon version (commit SHA, release tag, or `go install` pseudo-version)
- The flag set and daemon endpoint used to reproduce
- A minimal reproduction (curl command against `/api/…` or `/tela/…`, or a Go snippet importing `pkg/gnomes`)
- Your assessment of severity and suggested remediation

## Scope

In-scope:

- Anything in `cmd/hypergnomon`, `api/`, `indexer/`, `storage/`, `structures/`, `pool/`, `eventbus/`, `rpc/`, `pkg/gnomes/`
- TELA content-server byte-correctness bugs that cause HyperGnomon to serve content that does not match the on-chain SC code
- `fileCheckC/S` signature-verification bugs (v1.1+) that accept invalid signatures or reject valid ones
- Go memory-safety issues, DoS vectors against the indexer from a malicious daemon, cache-poisoning attacks

Out-of-scope:

- Issues in `civilware/Gnomon`, `civilware/tela`, DERO daemon (`derod`), or any consumer dApp built on top of HyperGnomon — report those upstream
- Self-inflicted DoS from misconfigured flags (e.g. `--num-parallel-blocks=1024` against a rate-limited remote daemon)
- TELA spec-level concerns (how signatures should be computed, what key layout is canonical) — file those with `civilware/tela`

## Response timeline

- **Acknowledgment**: within 48 hours
- **Initial assessment + severity classification**: within 1 week
- **Fix / disclosure**: depends on severity. Critical: patch release within 1–2 weeks. High: next minor release. Medium/low: next scheduled release.

## Supported versions

| Version | Supported |
|---|---|
| `main` | yes |
| v1.x | yes |
| v0.9.x | security fixes only until v1.1 ships |
| < v0.9 | no |

## Disclosure

Once a fix has shipped, we publish a GitHub Security Advisory with acknowledgment to the reporter (opt-in). Embargo length is negotiated with the reporter on a case-by-case basis.
