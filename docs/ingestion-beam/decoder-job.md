# Decoder Job

A job for normalizing ingestion messages. Defined in the `com.mozilla.telemetry.Decoder` class ([source](https://github.com/mozilla/gcp-ingestion/blob/master/ingestion-beam/src/main/java/com/mozilla/telemetry/Decoder.java)).

## Pipeline

The Decoder is a single Beam pipeline, configured per Dataflow instance, that reads
one input topic. The input wire format is selected at deploy time by two mutually
exclusive flags; `Edge` is the default when neither is set:

- **Edge** (default, no flag) - pings delivered over HTTP by `ingestion-edge`.
- `--logIngestionEnabled=true` - Glean server-side pings wrapped in Cloud Logging
  `LogEntry` messages (the `structured-logging` input).
- `--directPubsubEnabled=true` - structured pings published directly to Pub/Sub by
  server-side producers.

See the [decoder service specification](../architecture/decoder_service_specification.md)
for the ingestion sources and wire formats in more detail.

## Transforms

The Decoder is a single linear chain of transforms, defined in
[`Decoder.java`](https://github.com/mozilla/gcp-ingestion/blob/master/ingestion-beam/src/main/java/com/mozilla/telemetry/Decoder.java).
Every message passes through the whole chain; the transforms specific to the alternative
input formats are inserted as extra stages rather than as separate branches, and they
no-op on messages that do not need them (usually by checking whether
`submission_timestamp` or the geo attributes are already set). The same checks make the
chain safe to re-run when reprocessing messages from the error output.

The order is:

1. [Parse URI](#parse-uri), extraction only
1. [Parse Proxy](#parse-proxy)
1. [GeoISP Lookup](#geoisp-lookup)
1. [GeoIP Lookup](#geoip-lookup)
1. [Decompress](#decompress)
1. With `--logIngestionEnabled` only:
   [Extract IP From LogEntry](#extract-ip-from-logentry), then
   [GeoISP Lookup](#geoisp-lookup) and [GeoIP Lookup](#geoip-lookup) a second time, then
   [Parse LogEntry](#parse-logentry)
1. With `--directPubsubEnabled` only:
   [Stamp Submission Timestamp](#stamp-submission-timestamp)
1. [Parse URI](#parse-uri) failures are routed to the error output here
1. [Limit Payload Size](#limit-payload-size)
1. [Parse Payload](#parse-payload)
1. [Parse User Agent](#parse-user-agent)
1. [Normalize Attributes](#normalize-attributes)
1. [Sanitize Attributes](#sanitize-attributes)
1. [Write Metadata Into the Payload](#write-metadata-into-the-payload)

Parse URI is deliberately split in two. Attributes are extracted at the top of the chain,
but parse failures are not routed to the error output until after the geo lookups have
removed the IP address, so that no raw IP can reach the error output; see
[#1096](https://github.com/mozilla/gcp-ingestion/issues/1096).

### Parse URI

Attempt to extract `document_namespace`, `document_type`, `document_version` and
`document_id` attributes from `uri` (plus `app_name`, `app_version`,
`app_update_channel` and `app_build_id` for legacy telemetry URIs). As described
above, messages that fail to parse are sent to the configured error output at a later
point in the chain rather than here.

### Parse Proxy

Trim load balancer entries from the `x_forwarded_for` attribute, so that the geo lookups
below read the immediate sending client IP from the end of the list.

1. If `x_forwarded_for` holds more than one entry, remove the last two - the global
   forwarding rule IP appended by the Google load balancer, and the load balancer IP
   then appended by our nginx setup
1. Remove any further trailing entries indicated by the `geoip_skip_entries` pipeline
   metadata for the document type, read from the configured schemas location
1. Remove the `remote_addr` attribute
1. Remove any `null` values added to attributes

### GeoISP Lookup

1. Return the message unmodified if `isp_name` is already set (the message is likely
   being reprocessed from the error output)
1. Take the client IP as the _last_ value of the `x_forwarded_for` attribute
1. Record the database build date as `isp_db_version`
1. Execute the following steps until one fails and ignore the exception
   1. Parse the IP using `InetAddress.getByName`
   1. Lookup the IP in the configured `GeoIP2-ISP.mmdb`
   1. Extract the ISP as `isp_name` and the organization as `isp_organization`
1. Remove any `null` values added to attributes

### GeoIP Lookup

1. Return the message unmodified if `geo_country` is already set (the message is likely
   being reprocessed from the error output)
1. Take the client IP as the _last_ value of the `x_forwarded_for` attribute
   (Parse Proxy has already removed the load balancer entries from the end of the list)
1. Record the database build date as `geo_db_version`
1. Execute the following steps until one fails and ignore the exception
   1. Parse the IP using `InetAddress.getByName`
   1. Lookup the IP in the configured `GeoIP2-City.mmdb`
   1. Extract `country.iso_code` as `geo_country`
   1. Extract `city.name` as `geo_city`, and `location.metro_code` as `geo_dma_code`,
      if `cities15000.txt` is not configured or `city.geo_name_id` is in the
      configured `cities15000.txt`
   1. Extract `subdivisions[0].iso_code` as `geo_subdivision1`
   1. Extract `subdivisions[1].iso_code` as `geo_subdivision2`
1. Remove the `x_forwarded_for` attribute
1. Remove any `null` values added to attributes

### Decompress

Attempt to decompress payload with gzip, on failure pass the message through
unmodified (assuming it was not compressed). When decompression succeeds, set
`client_compression` to `gzip` if it is not already set. A
`predecompress_submission_bytes` counter is maintained per document type.

### Extract IP From LogEntry

Only applied with `--logIngestionEnabled`. Move the IP address out of the Cloud Logging
`LogEntry` payload into the `x_forwarded_for` attribute and remove it from the payload,
for consistency with the other inputs and so that it cannot reach the error tables.
Messages that already have `submission_timestamp` set come from the edge server and are
passed through unmodified.

GeoISP Lookup and GeoIP Lookup are then applied a second time, since this is the first
point at which a client IP is available for these messages. They no-op for messages that
already went through the lookups above.

### Parse LogEntry

Only applied with `--logIngestionEnabled`. Transform Glean server-side pings delivered
via Cloud Logging into a format compatible with structured ingestion, routing invalid
log entries to the error output. Messages that already have `submission_timestamp` set
come from the edge server and are passed through unmodified.

### Stamp Submission Timestamp

Only applied with `--directPubsubEnabled`, for structured pings published directly to
Pub/Sub.

1. Route messages missing any of `document_namespace`, `document_type`,
   `document_version` or `document_id` to the error output; the publisher is expected to
   set these as message attributes
1. Set `submission_timestamp` from the Pub/Sub `publishTime`, unless it is already set -
   edge-published and reprocessed messages keep their original timestamp

### Limit Payload Size

Route messages whose payload exceeds 8 MB to the error output; see
[#776](https://github.com/mozilla/gcp-ingestion/issues/776).

### Parse Payload

1. Parse the message body as a `UTF-8` encoded JSON payload
1. Drop specific fields or entire messages that match a specific set of signatures
   for toxic data that we want to make sure we do not store
   - Maintain counter metrics for each type of dropped message
1. Validate the payload structure based on the JSON schema for the specified
   document type
   - Invalid messages are routed to error output
1. Extract some additional attributes such as `client_id` and `os_name`
   based on the payload contents

### Parse User Agent

Attempt to extract browser, browser version, and os from the `user_agent`
attribute, drop any nulls, and remove `user_agent` from attributes.

### Normalize Attributes

Add canonical `normalized_*` attributes derived from existing attributes, for each one
that is present: `app_update_channel` to `normalized_channel`, `os` to `normalized_os`,
`os_version` to `normalized_os_version`, `app_name` to `normalized_app_name`, and
`geo_country` to `normalized_country_code`. Values that do not match a known form
normalize to `Other`. The source attributes are left in place.

### Sanitize Attributes

Alter attributes per document type according to the pipeline metadata in the configured
schemas location, to avoid storing data that may be overly identifying.

1. Truncate `submission_timestamp` to the configured
   `submission_timestamp_granularity`, if set
1. Apply any configured `override_attributes`, setting each named attribute to the given
   value, or removing the attribute when the value is `null`

### Write Metadata Into the Payload

Add a nested `metadata` field and several `normalized_*` attributes into the
payload body.

## Executing

Decoder jobs are executed the [same way as sink jobs](../sink-job/#executing)
but with a few extra flags:

- `-Dexec.mainClass=com.mozilla.telemetry.Decoder`
  - For Dataflow Flex Templates, change the `docker-compose` build argument to
    `--build-arg FLEX_TEMPLATE_JAVA_MAIN_CLASS=com.mozilla.telemetry.Decoder`
- `--geoCityDatabase=/path/to/GeoIP2-City.mmdb`
- `--geoCityFilter=/path/to/cities15000.txt` (optional)

To download the [GeoLite2 database](https://dev.maxmind.com/geoip/geoip2/geolite2/),
you need to [register for a MaxMind account](https://www.maxmind.com/en/geolite2/signup)
to obtain a license key. After generating a new license key, set `MM_LICENSE_KEY` to
your license key.

Example:

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"remote_addr":"63.245.208.195"}}' > tmp/input.json

# Download `cities15000.txt`, `GeoLite2-City.mmdb`, and `schemas.tar.gz`
./bin/download-cities15000
./bin/download-schemas

export MM_LICENSE_KEY="Your MaxMind License Key"
./bin/download-geolite2


# do geo lookup on messages to stdout
./bin/mvn compile exec:java -Dexec.mainClass=com.mozilla.telemetry.Decoder -Dexec.args="\
    --geoCityDatabase=GeoLite2-City.mmdb \
    --geoCityFilter=cities15000.txt \
    --schemasLocation=schemas.tar.gz \
    --inputType=file \
    --input=tmp/input.json \
    --outputType=stdout \
    --errorOutputType=stderr \
"

# check the DecoderOptions help page for options specific to Decoder
./bin/mvn compile exec:java -Dexec.args=--help=DecoderOptions
"
```
