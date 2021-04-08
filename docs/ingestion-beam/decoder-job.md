# Decoder Job

A job for normalizing ingestion messages. Defined in the `com.mozilla.telemetry.Decoder` class ([source](https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/java/com/mozilla/telemetry/Decoder.java)).

## Transforms

These transforms are currently executed against each message in order.

### GeoIP Lookup

1. Extract `ip` from the `x_forwarded_for` attribute
   - when the `x_pipeline_proxy` attribute is not present, use the
     second-to-last value (since the last value is a forwarding rule IP
     added by Google load balancer)
   - when the `x_pipeline_proxy` attribute is present, use the third-to-last
     value (since the tee introduces an additional proxy IP)
   - fall back to the `remote_addr` attribute, then to an empty string
1. Execute the following steps until one fails and ignore the exception
   1. Parse `ip` using `InetAddress.getByName`
   1. Lookup `ip` in the configured `GeoIP2City.mmdb`
   1. Extract `country.iso_code` as `geo_country`
   1. Extract `city.name` as `geo_city` if `cities15000.txt` is not configured
      or `city.geo_name_id` is in the configured `cities15000.txt`
   1. Extract `subdivisions[0].iso_code` as `geo_subdivision1`
   1. Extract `subdivisions[1].iso_code` as `geo_subdivision2`
1. Remove the `x_forwarded_for` and `remote_addr` attributes
1. Remove any `null` values added to attributes

### Parse URI

Attempt to extract attributes from `uri`, on failure send messages to the
configured error output.

### Decompress

Attempt to decompress payload with gzip, on failure pass the message through
unmodified.

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
