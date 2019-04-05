#!/usr/bin/env python

import argparse
import tarfile
import tempfile
import os
import json
import logging
import base64

INGESTION_BEAM_ROOT = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
)


# The payload object must be an object. This behavior is enforced by the utility
# function found in com.mozilla.telemetry.util.Json
AVRO_SCHEMAS = {
    "schemas/namespace_0/foo/foo.1.avro.json": {
        "type": "record",
        "name": "payload",
        "fields": [{"name": "test_int", "type": "int"}],
    },
    "schemas/namespace_0/bar/bar.1.avro.json": {
        "type": "record",
        "name": "payload",
        "fields": [{"name": "test_int", "type": "int"}],
    },
    "schemas/namespace_1/baz/baz.1.avro.json": {
        "type": "record",
        "name": "payload",
        "fields": [{"name": "test_null", "type": "null"}],
    },
    "schemas/namespace_1/baz/baz.1.schema.json": {"type": "null"},
}


def main(output_path):
    tar_path = os.path.join(output_path, "avro-schema-test.tar.gz")
    logging.info("Writing tarfile to {}".format(tar_path))
    root = tempfile.mkdtemp()

    for path, schema in AVRO_SCHEMAS.items():
        # check that the folder exists
        prefix = os.path.join(root, os.path.dirname(path))
        if not os.path.exists(prefix):
            os.makedirs(prefix)
        # write the schema to the folder
        schema_path = os.path.join(root, path)

        logging.info("Generating {}".format(path))
        with open(schema_path, "w") as fp:
            json.dump(schema, fp)

    tf = tarfile.open(tar_path, mode="w:gz")
    # rename the temporary folder in the archive as the filename without the `.tar.gz` suffix
    toplevel = os.path.basename(tar_path).split(".")[0]
    tf.add(root, arcname=toplevel)
    tf.close()

    logging.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-path",
        type=str,
        default=os.path.join(INGESTION_BEAM_ROOT, "src/test/resources/testdata"),
    )
    args = parser.parse_args()
    main(args.output_path)
