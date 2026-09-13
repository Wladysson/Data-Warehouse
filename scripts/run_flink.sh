#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_JOB="${PROJECT_ROOT}/src/streaming/flink_job.py"
FLINK_CONFIG="${PROJECT_ROOT}/configs/flink/flink-conf.yaml"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Flink Streaming Job"
echo "=============================================="

if ! command -v flink >/dev/null 2>&1; then
    echo "Erro: flink não encontrado no PATH."
    exit 1
fi

if [[ ! -f "${FLINK_JOB}" ]]; then
    echo "Erro: job Flink não encontrado:"
    echo "${FLINK_JOB}"
    exit 1
fi

if [[ ! -f "${FLINK_CONFIG}" ]]; then
    echo "Erro: configuração do Flink não encontrada:"
    echo "${FLINK_CONFIG}"
    exit 1
fi

export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

echo "Job: ${FLINK_JOB}"
echo "Configuração: ${FLINK_CONFIG}"
echo "Iniciando processamento streaming..."

exec flink run \
    -py "${FLINK_JOB}" \
    -Dpython.client.executable=python3