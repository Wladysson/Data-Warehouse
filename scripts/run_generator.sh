```bash
#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${PROJECT_ROOT}"

echo "=============================================="
echo " E-commerce Event Generator"
echo "=============================================="

if [[ -f "${PROJECT_ROOT}/.venv/bin/activate" ]]; then
    source "${PROJECT_ROOT}/.venv/bin/activate"
elif [[ -f "${HOME}/venv/bin/activate" ]]; then
    source "${HOME}/venv/bin/activate"
else
    echo "Aviso: nenhum ambiente virtual encontrado."
fi

export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

echo "Iniciando gerador de eventos..."
python3 "${PROJECT_ROOT}/src/data_generator/gerador.py"
```
