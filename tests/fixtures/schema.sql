-- Shared test fixtures.
--
-- Creates the schema referenced by integration tests across the workspace:
--   - api.authors       (resolute macros + runtime tests, pg-wired basics)
--   - api.articles      (pg-wired / pg-pool JWT + RLS tests)
--   - web_anon role     (anonymous reader, sees only published rows)
--   - test_user role    (authenticated reader, sees drafts via RLS policy)
--
-- Must be applied to the postgrest_test database before running cargo test.
-- CI does this automatically (see .github/workflows/ci.yml). Locally, running
-- `docker compose up` mounts this file into /docker-entrypoint-initdb.d/ so
-- it runs once on first init. For bare clusters, pipe this file to psql.
--
-- Every statement is idempotent so it's safe to re-apply.

CREATE SCHEMA IF NOT EXISTS api;

-- Roles used by tests that exercise `SET LOCAL ROLE` for PostgREST-style
-- role switching. NOLOGIN because we only SET ROLE into them, never connect.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'web_anon') THEN
        CREATE ROLE web_anon NOLOGIN;
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'test_user') THEN
        CREATE ROLE test_user NOLOGIN;
    END IF;
END $$;

-- authors: Alice's bio mentions "Rust" so the nullable-column macro test
-- can assert the column round-trips a non-empty value.
CREATE TABLE IF NOT EXISTS api.authors (
    id   integer PRIMARY KEY,
    name text    NOT NULL,
    bio  text
);

INSERT INTO api.authors (id, name, bio) VALUES
    (1, 'Alice', 'Wrote the first chapter in Rust.'),
    (2, 'Bob',   NULL),
    (3, 'Carol', 'Finalized the manuscript.')
ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name,
        bio  = EXCLUDED.bio;

-- articles: seeded with a mix of Published/Draft rows so RLS policies have
-- something to filter on.
CREATE TABLE IF NOT EXISTS api.articles (
    id        integer PRIMARY KEY,
    title     text    NOT NULL,
    status    text    NOT NULL,
    author_id integer REFERENCES api.authors(id)
);

INSERT INTO api.articles (id, title, status, author_id) VALUES
    (1, 'Published Piece',   'Published', 1),
    (2, 'Draft in Progress', 'Draft',     1),
    (3, 'Another Article',   'Published', 2)
ON CONFLICT (id) DO UPDATE
    SET title     = EXCLUDED.title,
        status    = EXCLUDED.status,
        author_id = EXCLUDED.author_id;

GRANT USAGE  ON SCHEMA   api            TO web_anon, test_user;
GRANT SELECT ON api.authors             TO web_anon, test_user;
GRANT SELECT ON api.articles            TO web_anon, test_user;

-- RLS: web_anon sees only Published; test_user sees everything.
ALTER TABLE api.articles ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS articles_read_published ON api.articles;
CREATE POLICY articles_read_published ON api.articles
    FOR SELECT
    TO web_anon
    USING (status = 'Published');

DROP POLICY IF EXISTS articles_read_all ON api.articles;
CREATE POLICY articles_read_all ON api.articles
    FOR SELECT
    TO test_user
    USING (true);
