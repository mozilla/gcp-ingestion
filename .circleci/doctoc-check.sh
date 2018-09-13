#!/bin/bash

set -e

bash "$(dirname $0)/doctoc-run.sh"

# Exit with success code if doctoc modified no files.
git diff --name-only | grep '.md$' || exit 0

# Print instructions and fail this test.
echo "Some markdown files have outdated Table of Contents!"
echo "To fix, run ./bin/update-toc"
exit 1
