```bash
#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=============================================="
echo " E-commerce Big Data Pipeline"
echo "=============================================="
echo "Projeto: ${PROJECT_ROOT}"
echo

cd "${PROJECT_ROOT}"

echo "[1/4] Iniciando geração de eventos..."
"${PROJECT_ROOT}/scripts/run_generator.sh"

echo
echo "[2/4] Iniciando Flume..."
"${PROJECT_ROOT}/scripts/run_flume.sh"

echo
echo "[3/4] Iniciando processamento streaming com Flink..."
"${PROJECT_ROOT}/scripts/run_flink.sh"

echo
echo "[4/4] Iniciando processamento batch com Spark..."
"${PROJECT_ROOT}/scripts/run_spark.sh"

echo
echo "=============================================="
echo " Pipeline iniciado com sucesso"
echo "=============================================="
```
