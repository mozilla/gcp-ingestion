#!/usr/bin/env python3

import base64
import logging
import json
import os
import tarfile
from functools import partial

import boto3
import rapidjson  # python-rapidjson


INGESTION_BEAM_ROOT = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
)


def parse_schema_name(path):
    """Given a directory path to a json schema in the mps directory, generate
    the fully qualified name in the form `{namespace}.{doctype}.{docver}`."""
    elements = path.split("/")
    doctype, docver = elements[-1].split(".")[:-2]
    namespace = elements[-3]
    return f"{namespace}.{doctype}.{docver}"


def load_schemas(path):
    """return a dictionary containing "{namespace}.{doctype}.{docver}" to validator"""
    with tarfile.open(path, "r") as tf:
        tf.extractall()
        root = tf.getnames()[0]
    schemas = {}
    for root, _, files in os.walk(root + "/schemas"):
        for name in files:
            if name.endswith(".schema.json"):
                schemafile = os.path.join(root, name)
                name = parse_schema_name(schemafile)
                with open(schemafile, "r") as f:
                    schemas[name] = rapidjson.Validator(f.read())
    return schemas


def get_schema_name(key):
    # Example:
    # sanitized-landfill-sample/v3/submission_date_s3=20190308/namespace=webpagetest/doc_type=webpagetest-run/doc_version=1/part-00122-tid-2954272513278013416-c06a39af-9979-41a5-8459-76412a4554b3-650.c000.json
    params = dict([x.split("=") for x in key.split("/") if "=" in x])
    return ".".join(map(params.get, ["namespace", "doc_type", "doc_version"]))


def generate_document(namespace, doctype, docver, payload):
    document = {
        "attributeMap": {
            "document_namespace": namespace,
            "document_type": doctype,
            "document_version": docver,
        },
        # payload is already a string
        "payload": base64.b64encode(payload.encode("utf-8")).decode("utf-8"),
    }
    return json.dumps(document)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    os.chdir(INGESTION_BEAM_ROOT)
    output_file = "avro-landfill-integration.ndjson"
    schemas = load_schemas("schemas.tar.gz")

    bucket = "telemetry-parquet"
    prefix = "sanitized-landfill-sample/v3/submission_date_s3=20190328"
    s3 = boto3.client("s3")

    objs = s3.list_objects(Bucket=bucket, Prefix=prefix)
    keys = [obj["Key"] for obj in objs["Contents"] if obj["Key"].endswith(".json")]

    fp = open(output_file, "w")
    for key in keys:
        schema_name = get_schema_name(key)
        if not schema_name in schemas:
            logging.info("schema does not exist for {}".format(schema_name))
            continue

        logging.info("Creating messages for {}".format(schema_name))
        data = (
            s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8").strip()
        )
        lines = data.split("\n")
        invalid = 0

        namespace, doctype, docver = schema_name.split(".")
        for line in lines:
            # each of the lines contains metadata with a content field
            content = json.loads(line).get("content")
            try:
                schemas[schema_name](content)
            except ValueError:
                invalid += 1
                continue
            pubsub_message = generate_document(namespace, doctype, docver, content)
            fp.write(pubsub_message + "\n")
        logging.info("Wrote {} documents".format(len(lines) - invalid, invalid))
    fp.close()
    logging.info("Done!")
