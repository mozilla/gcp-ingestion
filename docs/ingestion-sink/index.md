# Ingestion Sink

A Java application that runs in Kubernetes, reading input from Google Cloud
Pub/Sub and emitting records to batch-oriented outputs like GCS or BigQuery.
Defined in the `ingestion-sink` package
([source](https://github.com/mozilla/gcp-ingestion/tree/main/ingestion-sink/)).

## Supported Input and Outputs

Supported inputs:

- Google Cloud PubSub

Supported outputs:

- Google Cloud PubSub
- Google Cloud Storage
- Google Cloud BigQuery

### Test Input and Output

Test inputs will stop when an exception is raised or the end of the pipe or
file is reached. Supported test inputs:

- `System.in` (stdin), by setting `INPUT_PIPE` to any of `-`, `0`, `in`,
  `stdin`, `/dev/stdin`
- A single file, by setting `INPUT_PIPE` to `/path/to/input_file`

Test outputs don't exercise batching and will write messages as newline
delimited JSON in the order they are received. Supported test outputs:

- `System.out` (stdout), by setting `OUTPUT_PIPE` to any of `-`, `1`, `out`,
  `stdout`, `/dev/stdout`
- `System.err` (stderr), by setting `OUTPUT_PIPE` to any of `2`, `err`,
  `stderr`, `/dev/stderr`
- A single file, by setting `OUTPUT_PIPE` to `/path/to/output_file`

## Configuration

All configuration is controlled by environment variables.

## Output Specification

Depending on the environment variables provided, the application will
automatically determine where to deliver messages.

If `OUTPUT_BUCKET` is specified without `BIG_QUERY_OUTPUT_MODE`, then messages
will be delivered to Google Cloud Storage.

If `OUTPUT_TOPIC` is specified without `OUTPUT_BUCKET` or
`BIG_QUERY_OUTPUT_MODE`, then messages will be delivered to Google Cloud
Pub/Sub.

If `OUTPUT_TABLE` is specified without `BIG_QUERY_OUTPUT_MODE` or with
`BIG_QUERY_OUTPUT_MODE=streaming`, then messages will be delivered to BigQuery
via the streaming API.

If `OUTPUT_TABLE` is specified with `BIG_QUERY_OUTPUT_MODE=file_loads`, then
messages will be delivered to Google Cloud Storage based on `OUTPUT_BUCKET` and
for each blob a notification will be delivered to Google Cloud Pub/Sub based on
`OUTPUT_TOPIC`. Separate instances of ingestion-sink must consume notifications
from Google Cloud Pub/Sub and deliver messages to BigQuery via load jobs.

If `OUTPUT_TABLE` is specified with `BIG_QUERY_OUTPUT_MODE=mixed`, then
messages will be delivered to BigQuery via both the streaming API and load
jobs, and `OUTPUT_BUCKET` is required. If `OUTPUT_TOPIC` is specified then it
will be used the same as with `BIG_QUERY_OUTPUT_MODE=file_loads`, otherwise
load jobs will be submitted by each running instance of ingestion-sink.

If none of the above configuration options are provided, then messages must be
notifications from `BIG_QUERY_OUTPUT_MODE=file_loads` or
`BIG_QUERY_OUTPUT_MODE=mixed`, and the blobs they indicate will be submitted to
BigQuery via load jobs.

### BigQuery

`OUTPUT_TABLE` must be a `tableSpec` of form `dataset.tablename`
or the more verbose `projectId.dataset.tablename`. The values can contain
attribute placeholders of form `${attribute_name}`. To set dataset to the
document namespace and table name to the document type, specify:

    OUTPUT_TABLE='${document_namespace}.${document_type}'

All `-` characters in the attributes will be converted to `_` per BigQuery
naming restrictions. Additionally, document namespace and type values will
be processed to ensure they are in snake case format (`untrustedModules`
becomes `untrusted_modules`).

Defaults for the placeholders using `${attribute_name:-default_value}`
are supported, but likely don't make much sense since it's unlikely that
there is a default table whose schema is compatible with all potential
payloads.

### Attribute placeholders

We support routing individual messages to different output locations based on
the `PubsubMessage` attribute map. Routing is accomplished by adding
placeholders of form `${attribute_name:-default_value}` to the path.

For example, to route based on a `document_type` attribute, your path might
look like:

    OUTPUT_BUCKET=gs://mybucket/mydocs/${document_type:-UNSPECIFIED}/myfileprefix

Messages with `document_type` of "main" would be grouped together and end up in
the following directory:

    gs://mybucket/mydocs/main/

Messages with `document_type` set to `null` or missing that attribute
completely would be grouped together and end up in directory:

    gs://mybucket/mydocs/UNSPECIFIED/

Note that placeholders _must_ specify a default value so that a poorly
formatted message doesn't cause a pipeline exception. A placeholder without a
default will result in an `IllegalArgumentException` on pipeline startup.

File-based outputs support the additional _derived_ attributes
`"submission_date"` and `"submission_hour"` which will be parsed from the value
of the `submission_timestamp` attribute if it exists. These can be useful for
making sure your output specification buckets messages into hourly directories.

The templating and default syntax used here is based on the
[Apache commons-text `StringSubstitutor`](https://commons.apache.org/proper/commons-text/javadocs/api-release/org/apache/commons/text/StringSubstitutor.html),
which in turn bases its syntax on common practice in bash and other Unix/Linux
shells. Beware the need for proper escaping on the command line (use `\$` in
place of `$`), as your shell may try to substitute in values for your
placeholders before they're passed to `Sink`.

[Google's PubsubMessage format](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
allows arbitrary strings for attribute names and values. We place the following restrictions
on attribute names and default values used in placeholders:

- attribute names may not contain the string `:-`
- attribute names may not contain curly braces (`{` or `}`)
- default values may not contain curly braces (`{` or `}`)

### Encoding

When writing messages to Google Cloud Storage or BigQuery, the message received
from Google Cloud Pub/Sub will be encoded as a JSON object.

When `OUTPUT_FORMAT` is unspecified or `raw`, messages will have bytes encoded as
a `"payload"` field with base64 encoding, and each attribute encoded as field.
This is the format used for `payload_bytes_raw.*` tables.

When `OUTPUT_FORMAT` is `decoded` messages will have bytes encoded as with
`OUTPUT_FORMAT=raw`, but attributes will be encoded using the nested metadata
format of decoded pings. This is the format used for `payload_bytes_decoded.*`
tables.

When `OUTPUT_FORMAT` is `payload` messages will have bytes decoded as JSON, and
will be transformed to coerce types and use snake case for compatibility with BigQuery.
This is the format used for `*_live.*` tables. This requires specifying a local
path to a gzipped tar archive that contains BigQuery table schemas as
`SCHEMAS_LOCATION`. If messages bytes are compressed then
`INPUT_COMPRESSION=gzip` must also be specified to ensure they are decompressed
before they are decoded as JSON.

When `OUTPUT_FORMAT` is `beam` messages will have bytes encoded as with
`OUTPUT_FORMAT=raw`, but attributes will be encoded as an `"attributeMap"` field
that contains a JSON object. This is the same format as produced by ingestion-beam
when using `--outputType=file` and `--outputFileFormat=json`.

### Google Cloud Storage file prefix

Google Cloud Storage files are named like:

    $OUTPUT_BUCKET/{UUID.randomUUID().toString()}.ndjson

or if `OUTPUT_TABLE` and `BIG_QUERY_OUTPUT_MODE` are specified:

    $OUTPUT_BUCKET/OUTPUT_TABLE=$OUTPUT_TABLE/{UUID.randomUUID().toString()}.ndjson

for example, with `OUTPUT_BUCKET=gs://test-bucket/test-output`:

    gs://test-bucket/test-output/ad715b24-7500-45e2-9691-cb91e3b9c2cc.ndjson

or with `OUTPUT_BUCKET=gs://test-bucket/test-output`, `OUTPUT_TABLE=my_dataset.raw_table`, and
`BIG_QUERY_OUTPUT_MODE=file_loads`:

    gs://test-bucket/test-output/OUTPUT_TABLE=my_dataset.raw_table/3b17c648-f8b9-4250-bdc1-5c2e472fdc26.ndjson

## Executing

### Locally with Docker

The provided `bin/mvn` script downloads and runs maven via docker so that less
setup is needed on the local machine. For prolonged development performance is
likely to be significantly better, especially in MacOS, if `mvn` is installed and
run natively without docker.

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' > tmp/input.ndjson

# consume messages from the test file, decode and re-encode them, and write to a directory
PASS_ENV="INPUT_PIPE=tmp/input.ndjson OUTPUT_PIPE=tmp/output.ndjson" ./bin/mvn compile exec:java

# check that the message was delivered
cat tmp/output.ndjson

# read message from stdin and write to stdout
cat tmp/input.ndjson | PASS_ENV="INPUT_PIPE=- OUTPUT_PIPE=-" ./bin/mvn compile exec:java

# read message from stdin and write to gcs
# note that $ needs to be escaped with \ to prevent shell substitution
cat tmp/input.ndjson | PASS_ENV="\
  INPUT_PIPE=- \
  OUTPUT_BUCKET=gs://my_bucket/\${document_type:-UNSPECIFIED}/ \
" ./bin/mvn compile exec:java
```

### Locally without Docker

If you install Java and maven, you can invoke `VAR=... mvn` in the above commands
instead of using `PASS_ENV="VAR=..." ./bin/mvn`. Be aware that Java 11 is the target JVM and some
reflection warnings may be thrown on newer versions. Though these are generally
harmless, you may need to comment out the
`<compilerArgument>-Werror</compilerArgument>` line in the `pom.xml` in the git
root.

```bash
# consume messages from the test file, decode and re-encode them, and write to a directory
INPUT_PIPE=tmp/input.ndjson OUTPUT_PIPE=tmp/output.ndjson mvn compile exec:java

# read message from stdin and write to stdout
cat tmp/input.ndjson | INPUT_PIPE=- OUTPUT_PIPE=- ./bin/mvn compile exec:java
```
