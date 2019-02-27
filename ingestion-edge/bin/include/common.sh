#!/usr/bin/env bash

set -eo pipefail

cd "$(dirname "$0")"/..

LINT_ARGS=( --black --docstyle --flake8 --mypy-ignore-missing-imports )

VENV_DIR="$PWD/venv/$(uname)"

# use venv unless disabled
if ${VENV:-true}; then PATH="$VENV_DIR/bin:$PATH"; fi
