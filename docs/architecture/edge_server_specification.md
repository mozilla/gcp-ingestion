# Edge Server Specification

This document specifies the behavior of the server that accepts submissions
from HTTP clients e.g. Firefox telemetry.

## General Data Flow

HTTP submissions come in from the wild, hit a load balancer, then optionally an
nginx proxy, then the HTTP edge server described in this document. Data is
accepted via a POST/PUT request from clients, which the server will wrap in a
[PubSub message](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)
and forward to Google Cloud PubSub, where any further processing, analysis, and storage will
be handled.

### Namespaces

Namespaces are used to control the processing of data from different types of
clients, from the metadata that is collected to the destinations where the data
is written, processed and accessible. Data sent to a namespace that is not
specifically configured is assumed to be in the
[non-Telemetry JSON format described here](https://docs.telemetry.mozilla.org/cookbooks/new_ping.html).
To request a new namespace configuration file a bug against the
[Data Platform Team](https://bugzilla.mozilla.org/enter_bug.cgi?product=Data%20Platform%20and%20Tools&component=Pipeline%20Ingestion)
with a short description of what the namespace will be used for and the desired
configuration options.

### Forwarding to the pipeline

The message is written to PubSub. If the message cannot be written to PubSub it
is written to a disk queue that will periodically retry writing to PubSub.

### Edge Server PubSub Message Schema

```
required string data                   // base64 encoded body
required group attributes {
  required string submission_timestamp // server time, ISO 8601 with microseconds and timezone "Z", example: "2018-03-12T21:02:18.123456Z"
  required string uri                  // example: "/submit/telemetry/6c49ec73-4350-45a0-9c8a-6c8f5aded0cf/main/Firefox/58.0.2/release/20180206200532"
  required string protocol             // example: "HTTP/1.1"
  required string method               // example: "POST"
  optional string args                 // query parameters, example: "v=4"
  // Headers
  optional string remote_addr          // usually a load balancer, example: "172.31.32.5"
  optional string content_length       // example: "4722"
  optional string date                 // example: "Mon, 12 Mar 2018 21:02:18 GMT"
  optional string dnt                  // example: "1"
  optional string host                 // example: "incoming.telemetry.mozilla.org"
  optional string user_agent           // example: "pingsender/1.0"
  optional string x_forwarded_for      // example: "10.98.132.74, 103.3.237.12"
  optional string x_pingsender_version // example: "1.0"
  optional string x_debug_id           // example: "my_debug_session_1"
  optional string x_pipeline_proxy     // time that the AWS->GCP tee received the message, example: "2018-03-12T21:02:18.123456Z"
}
```

## Server Request/Response

### GET Request

| Endpoint           | Description
| ------------------ | -----------
| `/__heartbeat__`   | check if service is healthy, and can reach PubSub or has space to store requests on disk
| `/__lbheartbeat__` | check if service is running
| `/__version__`     | return Dockerflow [version object](https://github.com/mozilla-services/Dockerflow/blob/master/docs/version_object.md)

### GET Response codes

* *200* - ok, check succeeded
* *204* - ok, check succeeded, no response body
* *404* - not found, check doesn't exist
* *500* - all is not well
* *507* - insufficient storage, should occur at some configurable limit before disk is full

### POST/PUT Request

Treat POST and PUT the same. Accept POST or PUT to URLs of the form

`^/submit/namespace/[/dimensions]$`

Example Telemetry format:

`/submit/telemetry/docId/docType/appName/appVersion/appUpdateChannel/appBuildID`

Specific Telemetry example:

`/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/nightly/20180328030202`

Example non-Telemetry format:

`/submit/namespace/docType/docVersion/docId`

Specific non-Telemetry example:

`/submit/eng-workflow/hgpush/1/2c3a0767-d84a-4d02-8a92-fa54a3376049`

Note that `docId` above is a unique document ID, which is used for de-duping
submissions. This is *not* intended to be the `clientId` field from Telemetry.
`docId` is required and must be a [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier).

#### Legacy Systems

Accept [TLS Error Reports](https://wiki.mozilla.org/SecurityEngineering/TLS_Error_Reports)
as POST or PUT to `/submit/sslreports` with no `docType`, `docVersion`, or
`docId`.

Accept [Stub Installer pings](https://firefox-source-docs.mozilla.org/browser/installer/windows/installer/StubPing.html)
as GET to `/stub/[docVersion]/[dimensions]`, with no `docType` or `docId`, and
over both HTTP and HTTPS. Use [POST/PUT Response codes](#postput-response-codes),
even though this endpoint is for GET requests.

### POST/PUT Response codes

* *200* - ok, request accepted into the pipeline
* *400* - bad request, for example an unencoded space in the URL
* *404* - not found, for example using a telemetry format URL in a non-telemetry namespace or vice-versa
* *411* - missing content-length header
* *413* - request body too large (note that if we have badly-behaved clients that retry on `4XX`, we should send back 202 on body/path too long).
* *414* - request path too long (see above)
* *500* - internal error
* *507* - insufficient storage, request failed because disk is full

### Other Response codes

* *405* - wrong request type (anything other than GET|POST|PUT)

## Other Considerations

### Compression

It is not desirable to do decompression on the edge node. We want to pass along
messages from the HTTP Edge node without "cracking the egg" of the payload.

We may also receive badly formed payloads, and we will want to track the
incidence of such things.

### Bad Messages

Since the actual message is not examined by the edge server the only failures
that occur are defined by the response status codes above. Messages are only
forwarded to the pipeline when a response code of 200 is returned to the
client.

### PubSub Topics

All messages that sent a response code of 200 are forwarded to a single PubSub
topic for decoding and landfill.

### GeoIP Lookups

No GeoIP lookup is performed by the edge server. If a client IP is available
then the PubSub consumer performs the lookup and then discards the IP before
the message is forwarded to a decoded PubSub topic.

### Data Retention

The edge server only stores data when PubSub cannot be reached, and removes
data after it is successfully written to PubSub. Down scaling will be disabled
for the Kubernetes pod and cluster when data is being stored, so that data is
not lost.

### Submission Timestamp Format

`submission_timestamp` is formatted as ISO 8601 with microseconds and timezone,
because it is compatible with BigQuery's
[Timestamp Type](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type),
so that the field doesn't need transformation.
