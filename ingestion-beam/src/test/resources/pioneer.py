#!/usr/bin/env python3
"""Script for generating resources for Pioneer v2.

To install the appropriate dependencies for this script:

    pip install jwcrypto
"""
import gzip
import json
from pathlib import Path

from jwcrypto import jwe, jwk

pioneer = Path(__file__).parent.resolve() / "pioneer"


def write_serialized(json_data, fp):
    json.dump(json.loads(json_data), fp, indent=2)


def generate_jwk(path: Path, name: str):
    """Generate a keypair.
    
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


def encrypt(payload, key: jwk.JWK, path: Path):
    """Encrypt and assemble a payload.

    https://jwcrypto.readthedocs.io/en/stable/jwe.html
    """
    if path.exists():
        print(f"{path} already exists, skipping...")
        return

    public_key = jwk.JWK.from_json(key.export_public())
    protected_header = {
        "alg": "RSA-OAEP-256",
        "enc": "A256CBC-HS512",
        "typ": "JWE",
        "kid": public_key.thumbprint(),
    }
    compressed = gzip.compress(json.dumps(payload).encode())
    jwetoken = jwe.JWE(compressed, recipient=public_key, protected=protected_header)
    with path.open("w") as fp:
        write_serialized(jwetoken.serialize(), fp)


def decrypt(key: jwk.JWK, path: Path):
    jwetoken = jwe.JWE()
    with path.open("rb") as fp:
        jwetoken.deserialize(fp.read())
    jwetoken.decrypt(key)
    payload = gzip.decompress(jwetoken.payload).decode()
    return json.loads(payload)


def main():
    key0 = generate_jwk(pioneer, "id_rsa_0")
    with (pioneer / "sample.cleartext.json").open("r") as fp:
        sample = json.load(fp)
    print(sample)

    path = pioneer / "sample.ciphertext.json"
    encrypt(sample, key0, path)
    result = decrypt(key0, path)

    assert sample == result, "payload does not match"


if __name__ == "__main__":
    main()
