#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Deploy Flink"
echo "=============================================="

if ! command -v docker >/dev/null 2>&1; then
    echo "Erro: Docker não encontrado no PATH."
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "Erro: Docker Compose não está disponível."
    exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
    echo "Erro: docker-compose.yml não encontrado:"
    echo "${COMPOSE_FILE}"
    exit 1
fi

echo "Iniciando infraestrutura Flink..."

docker compose \
    -f "${COMPOSE_FILE}" \
    up -d \
    flink

echo
echo "Aguardando inicialização do Flink..."
sleep 8

echo
echo "Verificando container Flink..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps flink

echo
echo "Flink iniciado."
echo "Execute scripts/check_flink.sh para validar o cluster."