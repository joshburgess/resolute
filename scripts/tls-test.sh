#!/usr/bin/env bash
#
# Helper for the pg-wired TLS runtime integration test.
#
# This is the local equivalent of the test-tls-runtime CI job. It generates a
# self-signed cert, builds a tiny derived Postgres image with TLS configured,
# starts it on port 54323, and runs cargo test pointing at it.
#
# Usage:
#   scripts/tls-test.sh up      # generate cert + start Postgres on :54323
#   scripts/tls-test.sh run     # run the test (assumes `up` already done)
#   scripts/tls-test.sh full    # up + run + down (recommended for one-shot)
#   scripts/tls-test.sh down    # tear down container and remove cert dir
#
# Requirements: openssl, docker. No sudo: file permissions are set inside
# the container by the Dockerfile, so the host doesn't need to chown to the
# postgres uid.
#
# The negative test in tls_test.rs also connects to the plaintext Postgres on
# :54322; bring that one up separately with `docker compose up -d`.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="${REPO_ROOT}/target/tls-fixture"
CONTAINER_NAME="pg-wired-tls-test"
IMAGE_TAG="pg-wired-tls-test:local"
PORT=54323
PG_IMAGE="postgres:17-alpine"

cmd_up() {
    rm -rf "${FIXTURE_DIR}"
    mkdir -p "${FIXTURE_DIR}"
    cd "${FIXTURE_DIR}"

    # Generate a self-signed root CA, then a server cert signed by that CA.
    # Rustls rejects a single self-signed cert that's both root and leaf
    # (`CaUsedAsEndEntity`), so we need a real chain.
    openssl req -newkey rsa:2048 -nodes -x509 -days 365 \
        -subj "/CN=resolute-test-ca" \
        -keyout ca.key -out ca.crt 2>/dev/null
    openssl req -newkey rsa:2048 -nodes \
        -subj "/CN=localhost" \
        -keyout server.key -out server.csr 2>/dev/null
    printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > server.ext
    openssl x509 -req -in server.csr \
        -CA ca.crt -CAkey ca.key -CAcreateserial \
        -days 365 -out server.crt \
        -extfile server.ext 2>/dev/null
    openssl x509 -in ca.crt -outform DER -out ca.crt.der

    cat > postgresql.conf <<'EOF'
listen_addresses = '*'
ssl = on
ssl_cert_file = '/etc/postgres-tls/server.crt'
ssl_key_file  = '/etc/postgres-tls/server.key'
password_encryption = 'scram-sha-256'
EOF

    cat > pg_hba.conf <<'EOF'
local all all trust
hostnossl all all 0.0.0.0/0 reject
hostnossl all all ::/0      reject
hostssl   all all 0.0.0.0/0 scram-sha-256
hostssl   all all ::/0      scram-sha-256
EOF

    cat > Dockerfile <<EOF
FROM ${PG_IMAGE}
COPY server.crt server.key /etc/postgres-tls/
COPY postgresql.conf pg_hba.conf /etc/postgresql/
RUN chown postgres:postgres \\
        /etc/postgres-tls/server.crt /etc/postgres-tls/server.key \\
        /etc/postgresql/postgresql.conf /etc/postgresql/pg_hba.conf \\
    && chmod 600 /etc/postgres-tls/server.key \\
    && chmod 644 /etc/postgres-tls/server.crt \\
        /etc/postgresql/postgresql.conf /etc/postgresql/pg_hba.conf
EOF

    docker build --quiet -t "${IMAGE_TAG}" . >/dev/null

    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    docker run -d --name "${CONTAINER_NAME}" \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=postgres \
        -p "${PORT}:5432" \
        "${IMAGE_TAG}" \
        -c config_file=/etc/postgresql/postgresql.conf \
        -c hba_file=/etc/postgresql/pg_hba.conf >/dev/null

    echo "Waiting for ${CONTAINER_NAME} on :${PORT}..."
    for _ in $(seq 1 30); do
        if docker exec "${CONTAINER_NAME}" pg_isready -U postgres -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    docker exec "${CONTAINER_NAME}" pg_isready -U postgres -h 127.0.0.1 -p 5432

    docker exec "${CONTAINER_NAME}" psql -U postgres -d postgres \
        -c "CREATE DATABASE postgrest_test;" >/dev/null
    docker exec -i "${CONTAINER_NAME}" psql -U postgres -d postgrest_test \
        < "${REPO_ROOT}/tests/fixtures/schema.sql" >/dev/null

    echo "TLS Postgres ready on 127.0.0.1:${PORT}."
    echo "  CA (DER): ${FIXTURE_DIR}/ca.crt.der"
}

cmd_down() {
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    rm -rf "${FIXTURE_DIR}"
    echo "Stopped ${CONTAINER_NAME} and removed ${FIXTURE_DIR}."
}

cmd_run() {
    cd "${REPO_ROOT}"
    RESOLUTE_TLS_TEST_ADDR="127.0.0.1:${PORT}" \
    RESOLUTE_TLS_TEST_CA_DER="${FIXTURE_DIR}/ca.crt.der" \
        cargo test -p pg-wired --features tls --test tls_test -- --nocapture
}

case "${1:-}" in
    up)   cmd_up ;;
    down) cmd_down ;;
    run)  cmd_run ;;
    full)
        trap cmd_down EXIT
        cmd_up
        cmd_run
        ;;
    *)
        echo "Usage: $0 {up|run|full|down}" >&2
        exit 1
        ;;
esac
