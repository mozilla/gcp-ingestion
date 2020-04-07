#!/usr/bin/env
"""Script for generating resources for Pioneer v2."""
# ! pip install cryptography
from base64 import b64encode, b64decode
from pathlib import Path
import json
import gzip
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes

pioneer = Path(__file__).parent.resolve() / "pioneer"


def generate_keypair(path: Path, name: str):
    """Generate a keypair.
    
    https://cryptography.io/en/latest/hazmat/primitives/asymmetric/rsa
    """
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    public_key = private_key.public_key()

    pvt_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    with (path / name).open("wb") as fp:
        fp.write(pvt_pem)
    with (path / f"{name}.pub").open("wb") as fp:
        fp.write(pub_pem)

    return private_key


def main():
    pvtkey0 = generate_keypair(pioneer, "id_rsa_0")
    with (pioneer / "sample.cleartext.json").open("r") as fp:
        sample = json.load(fp)
    print(sample)

    # based on the suggestions in the cryptography examples
    ciphertext = pvtkey0.public_key().encrypt(
        gzip.compress(json.dumps(sample).encode()),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    message = {"payload": b64encode(ciphertext).decode()}
    with (pioneer / "sample.ciphertext.json").open("w") as fp:
        json.dump(message, fp)

    # decode the data and assert equivalence
    with (pioneer / "sample.ciphertext.json").open("r") as fp:
        message = json.load(fp)
    ciphertext = b64decode(message["payload"])

    compressed = pvtkey0.decrypt(
        ciphertext,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    print(gzip.decompress(compressed).decode())


if __name__ == "__main__":
    main()
