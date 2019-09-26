# Republisher Job

A job for republishing subsets of decoded messages to new destinations. Defined in the `com.mozilla.telemetry.Republisher` class ([source](https://github.com/mozilla/gcp-ingestion/blob/master/ingestion-beam/src/main/java/com/mozilla/telemetry/Republisher.java)).

The primary intention is to produce smaller derived Pub/Sub topics so
that consumers that only need a specific subset of messages don't incur
the cost of reading the entire stream of decoded messages.

The Republisher has the additional responsibility of marking messages as seen
in `Cloud MemoryStore` for deduplication purposes. That functionality exists
here to avoid the expense of an additional separate consumer of the full
decoded topic.

## Capabilities

### Marking Messages As Seen

The job needs to connect to Redis in order to mark `document_id`s of consumed
messages as seen. The Decoder is able to use that information to drop duplicate
messages flowing through the pipeline.

### Debug Republishing

If `--enableDebugDestination` is set, messages containing an `x_debug_id`
attribute will be republished to a destination that's configurable at runtime.
This is currently expected to be a feature specific to structured ingestion,
so should not be set for `telemetry-decoded` input.

### Per-`docType` Republishing

If `--perDocTypeEnabledList` is provided, a separate producer will be created
for each `docType` specified in the given comma-separated list.
See the `--help` output for details on format.

### Per-Channel Sampled Republishing

If `--perChannelSampleRatios` is provided, a separate producer will be created
for each specified release channel. The messages will be randomly sampled
according to the ratios provided per channel.
This is currently intended as a feature only for telemetry data, so should
not be set for `structured-decoded` input.
See the `--help` output for details on format.

## Executing

Republisher jobs are executed the [same way as sink jobs](../sink-job/#executing)
but with a few differences in flags. You'll need to set the `mainClass`:

 * `-Dexec.mainClass=com.mozilla.telemetry.Republisher`

The `--outputType` flag is still required as in the sink, but the `--output`
configuration is ignored for the Republisher. Instead, there is a separate
destination configuration flag for each of the three republishing types.
For each type, there is an compile-time option that affects what publishers
are generated in the graph for the Dataflow job along with a runtime option
that determines the specific location (usually a topic name) for each publisher.

To enable debug republishing:

 * `--enableDebugDestination` (compile-time)
 * `--debugDestination=/some/pubsub/topic/path`

To enable per-`docType` republishing:

 * `--perDocTypeEnabledList=event,heartbeat` (compile-time)
 * `--perDocTypeDestination=/some/pubsub/topic/path/per-doctype-${document_namespace}-${document_type}` (compile-time)

To enable per-channel sampled republishing:

 * `--perChannelSampleRatios='{"nightly":1.0,"beta":0.1,"release":0.01}'` (compile-time)
 * `--perChannelDestination=/some/pubsub/topic/path/per-channel-${channel}` (compile-time)

Example:

```bash
# create a test input file
mkdir -p tmp/
echo '{"payload":"dGVzdA==","attributeMap":{"x_debug_id":"mysession"}}' > tmp/input.json

# Republish only messages with x_debug_id attribute to stdout.
./bin/mvn compile exec:java -Dexec.mainClass=com.mozilla.telemetry.Republisher -Dexec.args="\
    --inputType=file \
    --input=tmp/input.json \
    --outputType=stdout \
    --errorOutputType=stderr \
    --enableDebugDestination
"

# check the RepublisherOptions help page for options specific to Republisher
./bin/mvn compile exec:java -Dexec.args=--help=RepublisherOptions
"
```
