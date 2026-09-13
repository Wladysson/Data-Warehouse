#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " E-commerce Big Data Pipeline - Cleanup"
echo "=============================================="

echo "[1/5] Parando processos locais..."

pkill -f "src/data_generator/gerador.py" 2>/dev/null || true
pkill -f "flume-ng agent" 2>/dev/null || true
pkill -f "spark-submit" 2>/dev/null || true

if command -v flink >/dev/null 2>&1; then
    flink cancelall 2>/dev/null || true
fi

echo "Processos locais finalizados."

echo
echo "[2/5] Verificando Docker Compose..."

if ! command -v docker >/dev/null 2>&1; then
    echo "Aviso: Docker não encontrado. Pulando limpeza dos containers."
    exit 0
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "Aviso: Docker Compose não está disponível."
    exit 0
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
    echo "Aviso: docker-compose.yml não encontrado."
    exit 0
fi

echo
echo "[3/5] Parando serviços do pipeline..."

docker compose \
    -f "${COMPOSE_FILE}" \
    down

echo
echo "[4/5] Removendo containers órfãos..."

docker compose \
    -f "${COMPOSE_FILE}" \
    down \
    --remove-orphans

echo
echo "[5/5] Limpeza concluída."

echo
echo "=============================================="
echo " Pipeline finalizado e recursos temporários"
echo " do Docker Compose foram removidos."
echo "=============================================="