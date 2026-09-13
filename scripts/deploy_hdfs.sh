#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Deploy HDFS"
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

echo "Iniciando NameNode e DataNode..."

docker compose \
    -f "${COMPOSE_FILE}" \
    up -d \
    namenode \
    datanode

echo
echo "Aguardando inicialização do HDFS..."
sleep 5

echo
echo "Verificando containers HDFS..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps namenode datanode

echo
echo "HDFS iniciado."
echo "Execute scripts/check_hdfs.sh para validar o filesystem."