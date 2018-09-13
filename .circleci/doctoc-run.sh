#!/bin/bash

# Run doctoc to update tables of contents in markdown files.
# https://www.npmjs.com/package/doctoc

set -e

npm install -g --silent doctoc
doctoc . --notitle
