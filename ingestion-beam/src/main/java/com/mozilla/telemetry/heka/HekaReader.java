/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.heka;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.mozilla.telemetry.heka.Heka.Field.ValueType;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.util.Json;
import com.mozilla.telemetry.util.Time;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.xerial.snappy.Snappy;

public class HekaReader {

  public static PubsubMessage readHekaMessage(InputStream is) throws IOException {
    while (true) {
      // continue reading until we find a heka message or we reach the end of the
      // file
      // FIXME: add some code to keep track of errors, parseErrors does something
      // like this
      int cursor = is.read();
      if (cursor == -1) {
        return null;
      } else if (cursor == 0x1E) {
        // found separator, continue
        break;
      }
    }

    int headerLength = is.read();
    byte[] headerBuffer = new byte[headerLength];
    is.read(headerBuffer);
    Heka.Header header = Heka.Header.parseFrom(headerBuffer);

    // Parse unit separator
    if (is.read() != 0x1F) {
      throw new IOException("Invalid Heka Frame: missing unit separator");
    }

    // get message data
    byte[] messageBuffer = new byte[header.getMessageLength()];
    is.read(messageBuffer);

    byte[] uncompressedMessage;
    if (Snappy.isValidCompressedBuffer(messageBuffer)) {
      int uncompressedLength = Snappy.uncompressedLength(messageBuffer);
      uncompressedMessage = new byte[uncompressedLength];
      Snappy.uncompress(messageBuffer, 0, header.getMessageLength(), uncompressedMessage, 0);
    } else {
      uncompressedMessage = messageBuffer;
    }
    Heka.Message message = Heka.Message.parseFrom(uncompressedMessage);

    ObjectNode payload = null;
    // most messages produced by our infra should have a payload
    if (message.getPayload().length() > 0) {
      payload = Json.readObjectNode(message.getPayload().getBytes(StandardCharsets.UTF_8));
    } else {
      // if no payload, there should be a field called "submission" ("content" is also theoretically
      // possible, though it should not appear in the data we care about)
      FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName("fields");
      Object field = message.getField(fieldDescriptor);
      if (field instanceof List) {
        for (Object i : (List) field) {
          Heka.Field f = (Heka.Field) i;
          String key = f.getName();
          if (key.contentEquals("submission")) {
            payload = Json.readObjectNode(f.getValueBytes(0).toByteArray());
            break;
          }
        }
      }
    }
    if (payload == null) {
      throw new IOException("Unable to find viable payload for heka message");
    }

    FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName("fields");
    Object field = message.getField(fieldDescriptor);
    if (field instanceof List) {
      for (Object i : (List) field) {
        Heka.Field f = (Heka.Field) i;
        String key = f.getName();
        if (key.contentEquals("submission") || key.contentEquals("content")) {
          // these should be handled above, skip
          continue;
        }
        List<String> path = key.contains(".") ? Arrays.asList(key.split("\\."))
            : Arrays.asList("meta", key);
        String lastKey = path.get(path.size() - 1);
        ObjectNode target = payload;
        for (int j = 0; j < path.size() - 1; j++) {
          String p = path.get(j);
          target = (ObjectNode) (target.has(p) ? target.path(p) : target.putObject(p));
        }
        if ((f.getValueType() == Heka.Field.ValueType.STRING
            || f.getValueType() == Heka.Field.ValueType.BYTES) && target.isObject()) {
          String value;
          if (f.getValueType() == Heka.Field.ValueType.BYTES) {
            // assuming byte fields are parseable as UTF8
            // see code in moztelemetry and
            // https://bugzilla.mozilla.org/show_bug.cgi?id=1339421
            ByteString bytesValue = f.getValueBytes(0);
            value = bytesValue.toString(StandardCharsets.UTF_8);
          } else {
            value = f.getValueString(0);
          }

          if (value.length() > 0 && value.charAt(0) == '{') {
            target.set(lastKey, Json.readObjectNode(value.getBytes(StandardCharsets.UTF_8)));
          } else if (value.length() > 0 && value.charAt(0) == '[') {
            target.set(lastKey, Json.readArrayNode(value.getBytes(StandardCharsets.UTF_8)));
          } else if (value.matches("null")) {
            target.set(lastKey, NullNode.getInstance());
          } else {
            target.put(lastKey, value);
          }
        } else if (f.getValueType() == Heka.Field.ValueType.BOOL) {
          target.put(lastKey, f.getValueBool(0));
        } else if (f.getValueType() == ValueType.INTEGER) {
          target.put(lastKey, f.getValueInteger(0));
        } else if (f.getValueType() == Heka.Field.ValueType.DOUBLE) {
          target.put(lastKey, f.getValueDouble(0));
        }
      }
    }

    Map<String, String> attributes = new HashMap<>();
    attributes.put(Attribute.DOCUMENT_NAMESPACE, "telemetry");
    attributes.put(Attribute.HOST, message.getHostname());
    Optional.of(message.getTimestamp()).filter(v -> v > 0).map(Time::epochNanosToTimestamp)
        .ifPresent(s -> attributes.put(Attribute.SUBMISSION_TIMESTAMP, s));
    Optional.ofNullable(payload.remove("meta")).ifPresent(meta -> {
      Optional.ofNullable(meta.path("Date").textValue())
          .ifPresent(s -> attributes.put(Attribute.DATE, s));
      Optional.ofNullable(meta.path("DNT").textValue())
          .ifPresent(s -> attributes.put(Attribute.DNT, s));
      Optional.ofNullable(meta.path("X-PingSender-Version").textValue())
          .ifPresent(s -> attributes.put(Attribute.X_PINGSENDER_VERSION, s));
      Optional.ofNullable(meta.path("docType").textValue())
          .ifPresent(s -> attributes.put(Attribute.DOCUMENT_TYPE, s));
      Optional.ofNullable(meta.path("appBuildId").textValue())
          .ifPresent(s -> attributes.put(Attribute.APP_BUILD_ID, s));
      Optional.ofNullable(meta.path("appName").textValue())
          .ifPresent(s -> attributes.put(Attribute.APP_NAME, s));
      Optional.ofNullable(meta.path("appUpdateChannel").textValue())
          .ifPresent(s -> attributes.put(Attribute.APP_UPDATE_CHANNEL, s));
      Optional.ofNullable(meta.path("appVersion").textValue())
          .ifPresent(s -> attributes.put(Attribute.APP_VERSION, s));
      Optional.ofNullable(meta.path("documentId").textValue())
          .ifPresent(s -> attributes.put(Attribute.DOCUMENT_ID, s));
      Optional.ofNullable(meta.path("sourceVersion").textValue())
          .ifPresent(s -> attributes.put(Attribute.DOCUMENT_VERSION, s));
      Optional.ofNullable(meta.path("geoCity").textValue())
          .ifPresent(s -> attributes.put(Attribute.GEO_CITY, s));
      Optional.ofNullable(meta.path("geoCountry").textValue())
          .ifPresent(s -> attributes.put(Attribute.GEO_COUNTRY, s));
      Optional.ofNullable(meta.path("geoSubdivision1").textValue())
          .ifPresent(s -> attributes.put(Attribute.GEO_SUBDIVISION1, s));
      Optional.ofNullable(meta.path("geoSubdivision2").textValue())
          .ifPresent(s -> attributes.put(Attribute.GEO_SUBDIVISION2, s));
    });

    return new PubsubMessage(Json.asBytes(payload), attributes);
  }

}
