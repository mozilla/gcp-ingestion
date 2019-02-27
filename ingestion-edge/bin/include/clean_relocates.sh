#!/usr/bin/env bash

# list .pyc files that do not match "$PWD/"
RELOCATES="$(grep -rL "$PWD/" --include '*.pyc' --exclude-dir venv . || true)"

# remove .pyc files that appear to be relocated
test -z "$RELOCATES" || echo "$RELOCATES" | xargs rm
