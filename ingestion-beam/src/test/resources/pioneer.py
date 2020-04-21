#!/usr/bin/env python3
"""Script for generating resources for Pioneer v2.

To install the appropriate dependencies for this script:

    pip install jwcrypto
"""
import gzip
import json
from pathlib import Path
from jwcrypto import jwe, jwk
from base64 import b64decode, b64encode

resources = Path(__file__).parent.resolve()
pioneer = resources / "pioneer"


def write_serialized(json_data, fp):
    json.dump(json.loads(json_data), fp, indent=2)


def generate_jwk(path: Path, name: str):
    """Generate a keypair. If the keypair exists, return that instead.
    
    https://jwcrypto.readthedocs.io/en/stable/jwk.html
    """
    private_path = path / f"{name}.private.json"

    if private_path.exists():
        print(f"keys for {name} already exist, loading from file")
        with private_path.open("r") as fp:
            data = json.load(fp)
        return jwk.JWK(**data)
    else:
        key = jwk.JWK.generate(kty="RSA", size=2048)
        with (private_path).open("w") as fp:
            write_serialized(key.export_private(), fp)
        return key


def encrypt(payload, key: jwk.JWK, path: Path = None):
    """Encrypt and assemble a payload.

    https://jwcrypto.readthedocs.io/en/stable/jwe.html
    """
    public_key = jwk.JWK.from_json(key.export_public())
    protected_header = {
        "alg": "RSA-OAEP-256",
        "enc": "A256CBC-HS512",
        "typ": "JWE",
        "kid": public_key.thumbprint(),
    }
    compressed = gzip.compress(json.dumps(payload).encode())
    jwetoken = jwe.JWE(compressed, recipient=public_key, protected=protected_header)

    # ugly deserialization and serialization
    envelope = {"payload": jwetoken.serialize(compact=True)}
    if path:
        with path.open("w") as fp:
            write_serialized(json.dumps(envelope), fp)
    return envelope


def decrypt(key: jwk.JWK, path: Path):
    """Decrypt and throw away envelope"""
    with path.open("rb") as fp:
        envelope = json.load(fp)
    jwetoken = jwe.JWE()
    jwetoken.deserialize(envelope["payload"])
    jwetoken.decrypt(key)
    payload = gzip.decompress(jwetoken.payload).decode()
    return json.loads(payload)


def encrypt_decoder_integration_input(
    metadata_path: Path, input_path: Path, output_path: Path
):
    """The decoder-integration test assumes a `test` namespace and `test` type.
    This encrypts the payload with the key specified in the metadata path. This
    test is run locally, so KMS is not used to encrypt the private key."""

    with metadata_path.open() as fp:
        metadata = json.load(fp)
    assert (
        len(metadata) == 1
    ), "only a single entry corresponding to the `test` namespace should exist"

    # the private_key_uri is relative from ingestion-beam root for testing
    ingestion_beam = resources.parent.parent.parent
    with (ingestion_beam / metadata[0]["private_key_uri"]).open("r") as fp:
        key = jwk.JWK(**json.load(fp))

    with input_path.open() as fp:
        lines = fp.readlines()

    encrypted = []
    for line in lines:
        data = json.loads(line)
        payload = encrypt(json.loads(b64decode(data["payload"])), key)
        data["payload"] = b64encode(json.dumps(payload).encode()).decode()
        encrypted.append(data)

    with output_path.open("w") as fp:
        for line in encrypted:
            fp.write(json.dumps(line))
            fp.write("\n")


def main():
    for study_id in ["study-foo", "study-bar"]:
        key = generate_jwk(pioneer, study_id)

        with (pioneer / "sample.plaintext.json").open("r") as fp:
            sample = json.load(fp)

        path = pioneer / f"{study_id}.ciphertext.json"
        encrypt(sample, key, path)
        result = decrypt(key, path)

        assert sample == result, "payload does not match"

    encrypt_decoder_integration_input(
        pioneer / "metadata-decoder.json",
        resources / "testdata" / "decoder-integration" / "valid-input.ndjson",
        resources / "testdata" / "decoder-integration" / "pioneer.ndjson",
    )


if __name__ == "__main__":
    main()
