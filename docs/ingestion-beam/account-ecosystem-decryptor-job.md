# Account Ecosystem Decryptor Job

A job for decrypting identifiers used in Account Ecosystem Telemetry.
Defined in the `com.mozilla.telemetry.EcosystemDecryptor` class ([source](https://github.com/mozilla/gcp-ingestion/blob/master/ingestion-beam/src/main/java/com/mozilla/telemetry/EcosystemDecryptor.java)).
On startup, this job loads a bundle of private keys by referencing a set of
encrypted values and contacting GCP's KMS service to decrypt them and hold
them in memory.

## Transforms

These transforms are currently executed against each message in order.

### Decompress

Attempt to decompress payload with gzip, on failure pass the message through
unmodified.

### Decrypt Account Ecosystem Telemetry Identifiers

Strips encrypted AET fields from the message, decrypts them using the
loaded bundle of data pipeline private keys, and adds attributes for the
decrypted IDs:

1. Parse the message body as a `UTF-8` encoded JSON payload
   * Messages that fail to parse as JSON are dropped and a message logged 
1. Validate the payload structure based on the JSON schema for pre-decryption
   AET payloads
   * Messages that fail to validate are dropped and a message logged
1. Remove `ecosystem_anon_id` and `previous_ecosystem_anon_ids` values from
   the payload, decrypt them, and insert back into the payload as
   `ecosystem_user_id` and `previous_ecosystem_user_ids`
   * Any value that fails to parse as a valid JOSE JWE object in
     Compact Serialization form will cause the whole message to be dropped and
     a message logged

## Formats

The input `ecosystem_anon_id` and `previous_ecosystem_anon_ids` values must
be JOSE JWE objects in Compact Serialization form. The emitted `ecosystem_user_id`
and `previous_ecosystem_user_ids` values are expected to be UUID-like strings
of 32 hexadecimal characters.
