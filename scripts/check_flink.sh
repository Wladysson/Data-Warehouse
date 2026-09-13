#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Flink Health Check"
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

echo "[1/3] Verificando container Flink..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps flink

echo
echo "[2/3] Verificando disponibilidade do Flink..."

if ! docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T flink \
    flink --version >/dev/null 2>&1; then
    echo "Erro: Flink não respondeu corretamente."
    exit 1
fi

echo "Flink operacional."

echo
echo "[3/3] Verificando configuração do cluster..."

if docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T flink \
    test -f /opt/flink/conf/flink-conf.yaml; then
    echo "OK: configuração do Flink encontrada."
else
    echo "Aviso: flink-conf.yaml não encontrado no container."
fi

echo
echo "Flink operacional."