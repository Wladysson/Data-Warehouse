#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Spark Health Check"
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

echo "[1/3] Verificando container Spark..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps spark

echo
echo "[2/3] Verificando disponibilidade do Spark..."

if ! docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T spark \
    spark-submit --version >/dev/null 2>&1; then
    echo "Erro: Spark não respondeu corretamente."
    exit 1
fi

echo "Spark operacional."

echo
echo "[3/3] Verificando configuração do Spark..."

if docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T spark \
    test -f /opt/spark/conf/spark-defaults.conf; then
    echo "OK: configuração do Spark encontrada."
else
    echo "Aviso: spark-defaults.conf não encontrado no container."
fi

echo
echo "Spark operacional."