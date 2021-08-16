# Sink Job

A job for delivering messages between Google Cloud services. Defined in the `com.mozilla.telemetry.Sink` class ([source](https://github.com/mozilla/gcp-ingestion/blob/main/ingestion-beam/src/main/java/com/mozilla/telemetry/Sink.java)).

## Deprecated

This job has been replaced by [ingestion-sink](../ingestion-sink) for loading messages from Google Cloud PubSub into BigQuery.

## Supported Input and Outputs

Supported inputs:

- Google Cloud PubSub
- Google Cloud Storage

Supported outputs:

- Google Cloud PubSub
- Google Cloud Storage
- Google Cloud BigQuery
- stdout
- stderr

Supported error outputs, must include attributes and must not validate messages:

- Google Cloud PubSub
- Google Cloud Storage with JSON encoding
- stdout with JSON encoding
- stderr with JSON encoding

## Encoding

Internally messages are stored and transported as
[PubsubMessage](https://beam.apache.org/documentation/sdks/javadoc/2.6.0/org/apache/beam/sdk/io/gcp/pubsub/PubsubMessage.html).

Supported file formats for Cloud Storage are `json` or `text`. The `json` file
format stores newline delimited JSON, encoding the field `payload` as a base64
string, and `attributeMap` as an optional object with string keys and values.
The `text` file format stores newline delimited strings, encoding the field
`payload` as `UTF-8`.

We'll construct example inputs based on the following two values and their base64 encodings:

```
$ echo -en "test" | base64
dGVzdA==

$ echo -en "test\n" | base64
dGVzdAo=
```

Example `json` file:

    {"payload":"dGVzdA==","attributeMap":{"meta":"data"}}
    {"payload":"dGVzdAo=","attributeMap":null}
    {"payload":"dGVzdA=="}

The above file when stored in the `text` format:

    test
    test

    test

Note that the newline embedded at the end of the second JSON message results in
two text messages, one of which is blank.

## Output Path Specification

Depending on the specified output type, the `--output` path that you provide controls
several aspects of the behavior.

### BigQuery

When `--outputType=bigquery`, `--output` is a `tableSpec` of form `dataset.tablename`
or the more verbose `projectId:dataset.tablename`. The values can contain
attribute placeholders of form `${attribute_name}`. To set dataset to the
document namespace and table name to the document type, specify:

    --output='${document_namespace}.${document_type}'

All `-` characters in the attributes will be converted to `_` per BigQuery
naming restrictions. Additionally, document namespace and type values will
be processed to ensure they are in snake case format (`untrustedModules`
becomes `untrusted_modules`).

Defaults for the placeholders using `${attribute_name:-default_value}`
are supported, but likely don't make much sense since it's unlikely that
there is a default table whose schema is compatible with all potential
payloads.
Instead, records missing an attribute required by a placeholder
will be redirected to error output if no default is provided.

### Protocol

When `--outputType=file`, `--output` may be prefixed by a protocol specifier
to determine the
target data store. Without a protocol prefix, the output path is assumed
to be a relative or absolute path on the filesystem. To write to Google
Cloud Storage, use a `gs://` path like:

    --output=gs://mybucket/somdir/myfileprefix

### Attribute placeholders

We support `FileIO`'s "Dynamic destinations" feature (`FileIO.writeDynamic`) where
it's possible to route individual messages to different output locations based
on properties of the message.
In our case, we allow routing messages based on the `PubsubMessage` attribute map.
Routing is accomplished by adding placeholders of form `${attribute_name:-default_value}`
to the path.

For example, to route based on a `document_type` attribute, your path might look like:

    --output=gs://mybucket/mydocs/${document_type:-UNSPECIFIED}/myfileprefix

Messages with `document_type` of "main" would be grouped together and end up in
the following directory:

    gs://mybucket/mydocs/main/

Messages with `document_type` set to `null` or missing that attribute completely
would be grouped together and end up in directory:

    gs://mybucket/mydocs/UNSPECIFIED/

Note that placeholders _must_ specify a default value so that a poorly formatted
message doesn't cause a pipeline exception. A placeholder without a default will
result in an `IllegalArgumentException` on pipeline startup.

File-based outputs support the additional _derived_ attributes
`"submission_date"` and `"submission_hour"` which will be parsed from the value
of the `submission_timestamp` attribute if it exists.
These can be useful for making sure your output specification buckets messages
into hourly directories.

The templating and default syntax used here is based on the
[Apache commons-text `StringSubstitutor`](https://commons.apache.org/proper/commons-text/javadocs/api-release/org/apache/commons/text/StringSubstitutor.html),
which in turn bases its syntax on common practice in bash and other Unix/Linux shells.
Beware the need for proper escaping on the command line (use `\$` in place of `$`),
as your shell may try to substitute in values
for your placeholders before they're passed to `Sink`.

[Google's PubsubMessage format](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
allows arbitrary strings for attribute names and values. We place the following restrictions
on attribute names and default values used in placeholders:

- attribute names may not contain the string `:-`
- attribute names may not contain curly braces (`{` or `}`)
- default values may not contain curly braces (`{` or `}`)

### File prefix

Individual files are named by replacing `:` with `-` in the default format discussed in
the "File naming" section of Beam's
[`FileIO` Javadoc](https://beam.apache.org/documentation/sdks/javadoc/2.6.0/org/apache/beam/sdk/io/FileIO.html):

    $prefix-$start-$end-$pane-$shard-of-$numShards$suffix$compressionSuffix

In our case, `$prefix` is determined from the last `/`-delimited piece of the `--output`
path. If you specify a path ending in `/`, you'll end up with an empty prefix
and your file names will begin with `-`. This is probably not what you want,
so it's recommended to end your output path with a non-empty file prefix. We replace `:`
with `-` because [Hadoop can't handle `:` in file names](https://stackoverflow.com/q/48909921).

For example, given:

    --output=/tmp/output/out

An output file might be:

    /tmp/output/out--290308-12-21T20-00-00.000Z--290308-12-21T20-10-00.000Z-00000-of-00001.ndjson

## Executing

Note: `-Dexec.args` does not handle newlines gracefully, but bash will remove
`\` escaped newlines in `"`s.

### Locally

If you install Java and maven, you can invoke `mvn` in the following commands
instead of using `./bin/mvn`; be aware, though, that Java 8 is the target JVM
and some reflection warnings may be thrown on newer versions, though these are
generally harmless.

The provided `bin/mvn` script downloads and runs maven via docker so that less
setup is needed on the local machine. For prolonged development performance is
likely to be significantly better, especially in MacOS, if `mvn` is installed and
run natively without docker.

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' > tmp/input.json

# consume messages from the test file, decode and re-encode them, and write to a directory
./bin/mvn compile exec:java -Dexec.args="\
    --inputFileFormat=json \
    --inputType=file \
    --input=tmp/input.json \
    --outputFileFormat=json \
    --outputType=file \
    --output=tmp/output/out \
    --errorOutputType=file \
    --errorOutput=tmp/error \
"

# check that the message was delivered
cat tmp/output/*

# write message payload straight to stdout
./bin/mvn compile exec:java -Dexec.args="\
    --inputFileFormat=json \
    --inputType=file \
    --input=tmp/input.json \
    --outputFileFormat=text \
    --outputType=stdout \
    --errorOutputType=stderr \
"

# check the help page to see types of options
./bin/mvn compile exec:java -Dexec.args=--help

# check the SinkOptions help page for options specific to Sink
./bin/mvn compile exec:java -Dexec.args=--help=SinkOptions
```

### On Dataflow

```bash
# Pick a bucket to store files in
BUCKET="gs://$(gcloud config get-value project)"

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# Set credentials; beam is not able to use gcloud credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/creds.json"

# consume messages from the test file, decode and re-encode them, and write to a bucket
./bin/mvn compile exec:java -Dexec.args="\
    --runner=Dataflow \
    --inputFileFormat=json \
    --inputType=file \
    --input=$BUCKET/input.json \
    --outputFileFormat=json \
    --outputType=file \
    --output=$BUCKET/output \
    --errorOutputType=file \
    --errorOutput=$BUCKET/error \
"

# wait for the job to finish
gcloud dataflow jobs list

# check that the message was delivered
gsutil cat $BUCKET/output/*
```

### On Dataflow with Flex Templates

The [Dataflow templates documentation](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates) explains:

> Dataflow templates allow you to stage your pipelines on Google Cloud and [run
> them](https://cloud.google.com/dataflow/docs/templates/executing-templates) using the Google Cloud
> Console, the `gcloud` command-line tool, or REST API calls. [...] Flex Templates package the
> pipeline as a Docker image and stage these images on your project's Container Registry.

```bash
# pick the project to store the docker image in
PROJECT=$(gcloud config get-value project)"

# pick the region to run Dataflow jobs in
PROJECT=$(gcloud config get-value compute/region)"

# pick the bucket to store files in
BUCKET="gs://$PROJECT"

# configure gcloud credential helper for docker to push to GCR
gcloud auth configure-docker

# build the docker image for the Flex Template
export IMAGE=gcr.io/$PROJECT/ingestion-beam/sink:latest
docker-compose build --build-arg FLEX_TEMPLATE_JAVA_MAIN_CLASS=com.mozilla.telemetry.Sink
docker-compose push

# create the Flex Template
gcloud dataflow flex-template build \
    $BUCKET/sink/flex-templates/latest.json \
    --image $IMAGE \
    --sdk-language JAVA

# create a test input file
echo '{"payload":"dGVzdA==","attributeMap":{"host":"test"}}' | gsutil cp - $BUCKET/input.json

# run the dataflow Flex Template with gcloud
JOBNAME=file-to-file1
REGION=$(gcloud config get-value compute/region 2>&1 | sed 's/(unset)/us-central1/')
gcloud dataflow flex-template run $JOBNAME \
    --template-file-gcs-location=$BUCKET/sink/flex-templates/latest.json \
    --region=$REGION \
    --parameters=inputType=file \
    --parameters=outputType=file \
    --parameters=errorOutputType=file \
    --parameters=inputFileFormat=json \
    --parameters=outputFileFormat=json \
    --parameters=input=$BUCKET/input.json \
    --parameters=output=$BUCKET/output/ \
    --parameters=errorOutput=$BUCKET/error/

# get the job id
JOB_ID="$(gcloud dataflow jobs list --region=$REGION --filter=name=$JOBNAME --format='value(JOB_ID)' --limit=1)"

# wait for the job to finish
gcloud dataflow jobs show "$JOB_ID" --region=$REGION

# check that the message was delivered
gsutil cat $BUCKET/output/*
```

### In streaming mode

If `--inputType=pubsub`, Beam will execute in streaming mode, requiring some
extra configuration for file-based outputs. You will need to specify sharding like:

```
    --outputNumShards=10 \
    --errorOutputNumShards=10 \
```

or for Flex Templates:

```
    --parameters=outputNumShards=10 \
    --parameters=errorOutputNumShards=10 \
```

As discussed in the
[Beam documentation for `FileIO.Write#withNumShards`](https://beam.apache.org/releases/javadoc/2.8.0/org/apache/beam/sdk/io/FileIO.Write.html#withNumShards-int-),
batch mode is most efficient when the runner is left to determine sharding,
so `numShards` options should normally be left to their default of `0`, but
streaming mode can't perform the same optimizations thus an exception will be thrown
during pipeline construction if sharding is not specified.
As codified in [apache/beam/pull/1952](https://github.com/apache/beam/pull/1952),
the Dataflow runner suggests a reasonable starting point `numShards` is `2 * maxWorkers`
or 10 if `--maxWorkers` is unspecified.
