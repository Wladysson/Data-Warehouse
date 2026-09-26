#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_JOB="${PROJECT_ROOT}/src/batch/insights_job.py"
SPARK_CONFIG="${PROJECT_ROOT}/configs/spark/spark-defaults.conf"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " Spark Batch Job - Data Warehouse E-commerce"
echo "=============================================="

if ! command -v spark-submit >/dev/null 2>&1; then
    echo "Erro: spark-submit não encontrado no PATH."
    exit 1
fi

if [[ ! -f "${SPARK_JOB}" ]]; then
    echo "Erro: job Spark não encontrado:"
    echo "${SPARK_JOB}"
    exit 1
fi

export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

echo "Job: ${SPARK_JOB}"
echo "Iniciando processamento batch..."

if [[ -f "${SPARK_CONFIG}" ]]; then
    exec spark-submit \
        --properties-file "${SPARK_CONFIG}" \
        "${SPARK_JOB}" "$@"
else
    exec spark-submit "${SPARK_JOB}" "$@"
fi