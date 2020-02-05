# EcosystemDecryptor Job

A job for decrypting identifiers used in Account Ecosystem Telemetry.
Defined in the `com.mozilla.telemetry.EcosystemDecryptor` class ([source](https://github.com/mozilla/gcp-ingestion/blob/master/ingestion-beam/src/main/java/com/mozilla/telemetry/EcosystemDecryptor.java)).
On startup, this job loads a bundle of private keys by contacting GCP's KMS service.

## Transforms

These transforms are currently executed against each message in order.

### Decrypt Account Ecosystem Telemetry Identifiers

Strips encrypted AET attributes from the message, decrypts them using the
loaded bundle of data pipeline private keys, and adds attributes for the
decrypted IDs:

* Input attribute `x_ecosystem_anon_id` will be transformed to output attribute
  `ecosystem_user_id`
* Input attribute `x_prev_ecosystem_anon_id` (if present) will be transformed to
  output attribute `prev_ecosystem_user_id`

## Formats

The input `x_ecosystem_anon_id` and `x_prev_ecosystem_anon_id` attribute strings must
be JOSE JWE objects in Compact Serialization form. The emitted `ecosystem_user_id`
and `prev_ecosystem_user_id` attribute strings are UUIDs in 36-character hex standard form.
