#!/usr/bin/env bash

set -eo pipefail

cd "$(dirname "$0")"/..

LINT_ARGS=( --black --pydocstyle --flake8 --mypy-ignore-missing-imports )

VENV_DIR="$PWD/venv/$(uname)"
VENV_ACTIVATE="$VENV_DIR/bin/activate"

# use venv unless disabled
if ${VENV:-true} && test -e "$VENV_ACTIVATE" ; then source "$VENV_ACTIVATE"; fi
