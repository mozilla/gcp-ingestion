# Rally Decoder Job

The Rally decoder job is a variant of the [decoder job](./decoder-job.md)
defined in the `com.mozilla.telemetry.decoder.rally` package ([source][source]).
The decoder supports the [Rally data donation and sharing platform][website].
More information can be found on the [mana page][mana].

See [bug 1628539](https://bugzilla.mozilla.org/show_bug.cgi?id=1628539) for
initial implementation of the `pioneer-v2` decoder and [bug
1697342](https://bugzilla.mozilla.org/show_bug.cgi?id=1697342) for
implementation of the Glean.js encrypted pings.

## Overview

The Rally decoder includes three new options: `--pioneerEnabled`,
`--pioneerMetadataLocation`, and `--pioneerKmsEnabled`. And example of running
the job is as follows, which is captured in the `bin/run-pioneer-benchmark`
script.

```bash
mvn compile exec:java -Dexec.mainClass=com.mozilla.telemetry.Decoder -Dexec.args="\
    --runner=Dataflow \
    --profilingAgentConfiguration='{\"APICurated\": true}'
    --project=$project \
    --autoscalingAlgorithm=NONE \
    --workerMachineType=n1-standard-1 \
    --gcpTempLocation=$bucket/tmp \
    --numWorkers=2 \
    --region=us-central1 \
    --pioneerEnabled=true \
    --pioneerMetadataLocation=$bucket/$prefix/metadata/metadata.json \
    --pioneerKmsEnabled=false \
    --pioneerDecompressPayload=false \
    --geoIspDatabase=$bucket/$prefix/metadata/GeoIP2-ISP.mmdb  \
    --geoCityDatabase=$bucket/$prefix/metadata/GeoLite2-City.mmdb \
    --geoCityFilter=$bucket/$prefix/metadata/cities15000.txt \
    --schemasLocation=$bucket/$prefix/metadata/schemas.tar.gz \
    --inputType=file \
    --input=$bucket/$prefix/input/ciphertext/'part-*' \
    --outputType=file \
    --output=$bucket/$prefix/output/ciphertext/ \
    --errorOutputType=file \
    --errorOutput=$bucket/$prefix/error/ciphertext/ \
"
```

The `--pioneerEnabled` flag enables the transform in the decoder pipeline, which
comes before schema validation and payload processing. It uses the document
specified by the `--pioneerMetadataLocation` to locate information for the
`KeyStore`. The metadata location takes on the following form validated by [this
schema][keystore-meta]:

```json
[
  {
    "private_key_id": "rally-study-foo",
    "private_key_uri": "src/test/resources/jwe/rally-study-foo.private.json",
    "kms_resource_id": "projects/DUMMY_PROJECT_ID/locations/global/keyRings/test-ingestion-beam-integration/cryptoKeys/study-foo"
  },
  ....
]
```

The decoder reads the [JSON Web Key](https://tools.ietf.org/html/rfc7517) into
memory from the location in the `private_key_uri`. It can be encrypted using
[Cloud Key Management Service](https://cloud.google.com/kms/docs/quickstart) by
specifying `kms_resource_id` and enabling the `--pioneerKmsEnabled` flag.

The decoder decrypts pings that follow conventions for Rally or Pioneer pings.
All encryption and decryption takes place using [JSON Web Encryption
(JWE)](https://tools.ietf.org/html/rfc7516). An envelope is a piece of metadata
that surrounds the encrypted data. The Rally envelope is an object with a
payload field containing a JWE compact object. After decrypting the payload, the
ping takes the form of a Glean ping.  The document namespace (as per the [HTTP
Edge Server
Specification](https://docs.telemetry.mozilla.org/concepts/pipeline/http_edge_spec.html))
is used to fetch the key from memory.

The Pioneer ping's envelope uses the legacy mechanism of sending data through
the Telemetry pipeline as a `telemetry.pioneer-study.4` ping. In addition, the
envelope explicitly specifies the routing information for the ping. Finally, the
decoder constructs a PubSub message that includes the routing information and
decrypted message.

## Notable design decisions

Data SRE allocates each Rally study a JWK pair. The client must encode messages
with the key to reach the analysis environment. The client may not always
have the key, so there are exceptions for the enrollment and deletion pings. The
decoder will ignore the payload and extract the pioneer id for these document
types. 

[source]: https://github.com/mozilla/gcp-ingestion/tree/main/ingestion-beam/src/main/java/com/mozilla/telemetry/decoder/rally
[website]: https://rally.mozilla.org/)
[mana]: https://mana.mozilla.org/wiki/display/PIO/Mozilla+Rally+Home
[keystore-meta]: https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/resources/keystore-metadata.schema.json
