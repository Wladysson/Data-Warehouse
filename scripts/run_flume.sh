#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLUME_CONFIG="${PROJECT_ROOT}/configs/flume/flume-conf.properties"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Flume Ingestion Agent"
echo "=============================================="

if ! command -v flume-ng >/dev/null 2>&1; then
    echo "Erro: flume-ng não encontrado no PATH."
    exit 1
fi

if [[ ! -f "${FLUME_CONFIG}" ]]; then
    echo "Erro: configuração do Flume não encontrada:"
    echo "${FLUME_CONFIG}"
    exit 1
fi

echo "Configuração: ${FLUME_CONFIG}"
echo "Iniciando agente Flume..."

exec flume-ng agent \
    --conf "${PROJECT_ROOT}/configs/flume" \
    --conf-file "${FLUME_CONFIG}" \
    --name agent \
    -Dflume.root.logger=INFO,console