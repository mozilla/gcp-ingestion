#!/bin/bash
# Generate a new archive with avro schemas from the mozilla-pipeline-schema repository

cd "$(dirname "$0")/.." || exit

if [ ! -x  "$(command -v jsonschema-transpiler)" ]; then
    echo "jsonschema-transpiler is not installed"
    echo "Run 'cargo install --git https://github.com/acmiyaguchi/jsonschema-transpiler.git --branch dev'"
    exit 1
fi

if [[ ! -f schemas.tar.gz ]]; then
    echo "Run 'bin/download-schemas'"
    exit 1
fi

# store the current directory
finaldir=$(pwd)

# create a temporary directory for work
workdir=$(mktemp -d -t tmp.XXXXXXXXXX)
function cleanup {
  rm -rf "$workdir"
  echo "Running cleanup!"
}
trap cleanup EXIT
cd "$workdir" || exit

# find all jsonschemas
inschema="mozilla-pipeline-schemas-gcp-ingestion-tests"
tar -xf "$finaldir/schemas.tar.gz" -C "$workdir"
schemas=$(find $inschema/schemas -type file -name "*.schema.json")

outschema="mps-avro-ingestion-tests"

total=0
failed=0
# From a relative path to a jsonschema, transpile an avro schema in a mirrored directory.
# Requires: $outschema, $total, $failed
function generate_avro() {
    orig_path=$1
    # relative path to the root of the archive, using shell parameter expansion to strip the root
    relpath=$(dirname "${orig_path#*/}")
    avroname="$(basename "$orig_path" .schema.json).avro.json"
    outpath="$outschema/$relpath/$avroname"
    
    # create the folder to the new schema
    mkdir -p "$outschema/$relpath"

    if ! jsonschema-transpiler --type avro "$orig_path" > "$outpath" 2> /dev/null; then
        echo "Unable to convert $(basename "$orig_path")"
        rm "$outpath"
        ((failed++))
    fi
    ((total++))
}

for schema in $schemas; do
    generate_avro "$schema"
done
# see: https://github.com/acmiyaguchi/jsonschema-transpiler/pull/36
echo "$((total-failed))/$total sucessfully converted"

# prune folders from failed conversions
find "$outschema" -type d -empty -delete

# generate the final archive
tar -zcf avro-schema.tar.gz "$outschema"
cp avro-schema.tar.gz "$finaldir"
