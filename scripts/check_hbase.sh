#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " HBase Health Check"
echo "=============================================="

if ! command -v docker >/dev/null 2>&1; then
    echo "Erro: Docker não encontrado no PATH."
    exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
    echo "Erro: docker-compose.yml não encontrado:"
    echo "${COMPOSE_FILE}"
    exit 1
fi

echo "[1/3] Verificando container HBase..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps hbase

echo
echo "[2/3] Verificando disponibilidade do HBase..."

if ! docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T hbase \
    hbase shell -n <<< "status" >/dev/null 2>&1; then
    echo "Erro: HBase não respondeu corretamente."
    exit 1
fi

echo "HBase operacional."

echo
echo "[3/3] Verificando tabelas do pipeline..."

TABLES=(
    "streaming_metrics"
    "streaming_alerts"
    "event_realtime"
)

for table in "${TABLES[@]}"; do
    if docker compose \
        -f "${COMPOSE_FILE}" \
        exec -T hbase \
        hbase shell -n <<< "exists '${table}'" 2>/dev/null | grep -q "does exist"; then
        echo "OK: ${table}"
    else
        echo "Aviso: tabela não encontrada: ${table}"
    fi
done

echo
echo "HBase operacional."