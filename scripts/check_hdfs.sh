#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " HDFS Health Check"
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

echo "[1/3] Verificando containers Hadoop..."

docker compose \
    -f "${COMPOSE_FILE}" \
    ps namenode datanode

echo
echo "[2/3] Verificando NameNode..."

if ! docker compose \
    -f "${COMPOSE_FILE}" \
    exec -T namenode \
    hdfs dfsadmin -report >/dev/null 2>&1; then
    echo "Erro: NameNode não respondeu corretamente."
    exit 1
fi

echo "NameNode operacional."

echo
echo "[3/3] Verificando diretórios do pipeline..."

for path in /data/raw /data/processed; do
    if docker compose \
        -f "${COMPOSE_FILE}" \
        exec -T namenode \
        hdfs dfs -test -d "${path}"; then
        echo "OK: ${path}"
    else
        echo "Aviso: diretório não encontrado: ${path}"
    fi
done

echo
echo "HDFS operacional."