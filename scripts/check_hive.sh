#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Hive Health Check"
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

echo "[1/3] Verificando container Hive..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps hive

echo
echo "[2/3] Testando conexão com Hive..."

if ! docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T hive \
    hive -e "SELECT 1;" >/dev/null 2>&1; then
    echo "Erro: Hive não respondeu corretamente."
    exit 1
fi

echo "Hive operacional."

echo
echo "[3/3] Verificando databases do Data Warehouse..."

DATABASES=(
    "ecommerce"
    "ecommerce_staging"
    "ecommerce_analytics"
)

for database in "${DATABASES[@]}"; do
    if docker compose \
        -f "${COMPOSE_FILE}" \
        exec -T hive \
        hive -e "SHOW DATABASES;" 2>/dev/null | grep -qx "${database}"; then
        echo "OK: ${database}"
    else
        echo "Aviso: database não encontrada: ${database}"
    fi
done

echo
echo "Hive operacional."