# Security Policy

## Supported versions

Until a stable `1.0` release, only the latest published version of each crate
in the workspace receives security fixes. Once a `1.x` line exists, the most
recent minor release will be supported in addition to `main`.

| Crate            | Supported versions  |
|------------------|---------------------|
| `pg-wired`       | latest published    |
| `pg-pool`        | latest published    |
| `resolute`       | latest published    |
| `resolute-derive`| latest published    |
| `resolute-macros`| latest published    |
| `resolute-cli`   | latest published    |

## Reporting a vulnerability

Please do **not** open a public issue or pull request for security reports.

Send a private report through GitHub's "Report a vulnerability" workflow:

1. Open https://github.com/joshburgess/resolute/security/advisories/new
2. Describe the issue, the affected crate(s) and version(s), and a minimal
   reproduction if you have one.
3. If GitHub Security Advisories is not available to you, email the repository
   owner directly via the address listed on their GitHub profile.

You should expect an acknowledgement within 5 business days. If the report
turns out to be a real vulnerability, we will work with you on a coordinated
disclosure timeline (typically up to 90 days, less if a public exploit is
already circulating).

## Scope

In scope:

- Wire-protocol parsing in `pg-wired` (frame decoding, message length handling,
  authentication state machine, TLS negotiation when the `tls` feature is
  enabled).
- Connection lifecycle and state in `pg-pool` (use-after-return, connection
  reuse across sessions with leftover state).
- Compile-time query validation in `resolute-macros` (proc-macro input
  handling, offline cache files).
- Migration runner in `resolute-cli` (advisory-lock handling, migration file
  parsing).

Out of scope:

- Issues that require a malicious PostgreSQL server. We trust the database we
  connect to.
- Denial of service via resource exhaustion when the caller already controls
  query input (the database itself enforces limits there).
- Vulnerabilities in third-party dependencies. Please report those upstream.

## Handling

Confirmed vulnerabilities are tracked via GitHub Security Advisories. Patched
releases are published to crates.io and announced in `CHANGELOG.md` with a
short summary referencing the advisory ID once it is public.
