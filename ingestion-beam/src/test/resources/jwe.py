#!/usr/bin/env python3
"""Script for generating resources for the JWE decoder.

We reuse keys generated by the pioneer script, since there parameters remain the
same. The generate keys and and decrypt sections of the script can be ported
over without too much modifiction if necessary.

To install the appropriate dependencies for this script:

    pip install jwcrypto click

Here's a command to run:

    name=rally-study-foo
    python jwe.py encrypt jwe/${name}.private.json jwe/${name}.plaintext.json jwe/${name}.ciphertext.json
"""
import gzip
import json
from base64 import b64decode, b64encode
from copy import deepcopy
from pathlib import Path

import click
from jwcrypto import jwe, jwk


def write_serialized(json_data, fp):
    json.dump(json.loads(json_data), fp, indent=2)
    fp.write("\n")


def encrypt_data(payload, key, path):
    """Encrypt and assemble a payload.

    https://jwcrypto.readthedocs.io/en/stable/jwe.html
    """
    public_key = jwk.JWK.from_json(key.export_public())
    protected_header = {
        "alg": "ECDH-ES",
        "enc": "A256GCM",
        "typ": "JWE",
        "kid": public_key.thumbprint(),
    }
    compressed = gzip.compress(json.dumps(payload).encode())
    jwetoken = jwe.JWE(compressed, recipient=public_key, protected=protected_header)

    # just dump the data straight into the payload section
    envelope = dict(payload=jwetoken.serialize(compact=True))

    if path:
        with path.open("w") as fp:
            write_serialized(json.dumps(envelope), fp)
    return envelope


@click.group()
def entrypoint():
    pass


@entrypoint.command()
@click.argument("private_key", type=click.Path(dir_okay=False, exists=True))
@click.argument("input", type=click.Path(dir_okay=False, exists=True))
@click.argument("output", type=click.Path(dir_okay=False))
def encrypt(private_key, input, output):
    """Encrypt a message"""
    key = jwk.JWK(**json.loads(Path(private_key).read_text()))
    input_data = json.loads(Path(input).read_text())
    encrypt_data(input_data, key, Path(output))


if __name__ == "__main__":
    entrypoint()
