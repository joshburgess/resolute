-- Shared test fixtures.
--
-- Creates the `api.authors` table used by `resolute` integration tests,
-- `resolute-macros` compile-time metadata lookups, and the `pg-wired`
-- end-to-end tests.
--
-- Must be applied to the `postgrest_test` database before running
-- `cargo test`. CI does this automatically (see .github/workflows/ci.yml);
-- for local runs, pipe this file to psql against the test cluster.

CREATE SCHEMA IF NOT EXISTS api;

CREATE TABLE IF NOT EXISTS api.authors (
    id   integer PRIMARY KEY,
    name text    NOT NULL,
    bio  text
);

INSERT INTO api.authors (id, name, bio) VALUES
    (1, 'Alice',   'Wrote the first chapter.'),
    (2, 'Bob',     NULL),
    (3, 'Carol',   'Finalized the manuscript.')
ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name,
        bio  = EXCLUDED.bio;
