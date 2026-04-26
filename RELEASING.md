# Releasing

This repo publishes six crates to crates.io. They share a workspace and
several depend on each other, so the publish order matters.

Publish order (leaves first):

1. `pg-wired`
2. `pg-pool`
3. `resolute-derive`
4. `resolute-macros`
5. `resolute`
6. `resolute-cli`

`pg-wired-js` is `publish = false` and is released to npm separately.

## Pre-flight

Run from a clean checkout of `main`:

```bash
git checkout main && git pull --ff-only

# Working tree must be clean.
test -z "$(git status --porcelain)" || { echo "dirty tree"; exit 1; }

# Standard gate.
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -Dwarnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps --all-features

# Tests against a live Postgres on the canonical port (54322).
docker compose up -d
cargo test --workspace --all-targets
cargo test --workspace --doc

# MSRV check (matches CI).
rustup toolchain install 1.85 --profile minimal
cargo +1.85 check --workspace --locked --lib --bins

# Audit (matches CI).
cargo audit
```

## Pick the version

We release the six crates at a single workspace version until there is a
reason to fan out. Decide whether the change set is patch, minor, or major
under SemVer.

Before bumping, scan the diff against the previous tag for breaking changes:

```bash
git log --oneline v$(cat VERSION 2>/dev/null || echo 0.1.0)..HEAD
```

If any commit changes a public signature, removes a public item, or alters
runtime behavior in a way users could observe, treat the release as breaking
(major while pre-1.0 means a minor bump per the CHANGELOG note).

## Bump and changelog

1. Update `[workspace.package]` `version` in the root `Cargo.toml`.
2. Update each crate's `[dependencies.<sibling>]` entry to the new version
   (path stays). Example:
   ```toml
   pg-wired = { path = "../pg-wired", version = "0.2.0" }
   ```
3. Update `CHANGELOG.md`: rename `[Unreleased]` to the new version with the
   release date, add an empty `[Unreleased]` block on top, update the link
   references at the bottom.
4. Run `cargo build --workspace` to refresh `Cargo.lock`.
5. Commit:
   ```
   Release vX.Y.Z

   <one paragraph summary, no AI attribution>
   ```

## Tag

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

## Publish

Publish in the order listed at the top. After each `cargo publish`, wait for
the index update before publishing the next crate (crates.io is usually
ready in under 60 seconds).

```bash
cargo publish -p pg-wired
cargo publish -p pg-pool
cargo publish -p resolute-derive
cargo publish -p resolute-macros
cargo publish -p resolute
cargo publish -p resolute-cli
```

If a publish fails partway through, do **not** retry the earlier crates.
Their previous version on crates.io is unchanged. Fix the failing crate and
publish from that point onward.

For a dry run that catches packaging issues without touching crates.io:

```bash
for c in pg-wired pg-pool resolute-derive resolute-macros resolute resolute-cli; do
  cargo publish -p "$c" --dry-run || break
done
```

Note that `--dry-run` cannot resolve the new version of an as-yet-unpublished
sibling, so dry-run failures of the form "no matching version" for a
workspace sibling are expected during a multi-crate bump.

## GitHub release

Create a GitHub release from the tag. Body is the relevant section of
`CHANGELOG.md` plus a "Crates" line linking each crate's docs.rs entry.

## Post-release

- Verify each crate appears on crates.io and docs.rs builds successfully.
- Open an issue for any follow-ups deferred during the release.
- Bump `[Unreleased]` if the placeholder block was not added in the release
  commit.

## Yanking

If a release is broken, yank from crates.io:

```bash
cargo yank --version X.Y.Z -p <crate>
```

Yanking does not delete the crate; it prevents new dependents from picking it
up. Always publish a fixed `X.Y.(Z+1)` rather than relying on yanking alone.
