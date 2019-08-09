/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.heka;

import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.xerial.snappy.Snappy;

public class HekaReader {

  private static void insertPath(JSONObject o, List<String> s, Object v) {
    String key = s.get(0);
    if (s.size() == 1) {
      o.put(key, v);
    } else {
      JSONObject newValue = o.has(key) ? o.getJSONObject(key) : new JSONObject("{}");
      insertPath(newValue, s.subList(1, s.size()), v);
      o.put(key, newValue);
    }
  }

  private static JSONObject readHekaMessage(InputStream is) throws IOException {
    while (true) {
      // continue reading until we find a heka message or we reach the end of the
      // file
      // FIXME: add some code to keep track of errors, parseErrors does something
      // like this
      int cursor = is.read();
      if (cursor == -1) {
        return null;
      } else if (cursor == 0x1E) {
        // found seperator, continue
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

    int uncompressedLength = Snappy.uncompressedLength(messageBuffer);
    byte[] uncompressedMessage = new byte[uncompressedLength];
    Snappy.uncompress(messageBuffer, 0, header.getMessageLength(), uncompressedMessage, 0);
    Heka.Message message = Heka.Message.parseFrom(uncompressedMessage);

    JSONObject payload = new JSONObject(new String(message.getPayload().getBytes()));

    insertPath(payload, Arrays.asList("meta", "Hostname"), message.getHostname());
    insertPath(payload, Arrays.asList("meta", "Timestamp"), new Long(message.getTimestamp()));
    insertPath(payload, Arrays.asList("meta", "Type"), message.getDtype());

    FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName("fields");
    Object field = message.getField(fieldDescriptor);
    if (field instanceof List) {
      for (Object i : (List) field) {
        Heka.Field f = (Heka.Field) i;
        String key = (String) f.getName();
        List<String> path = key.contains(".") ? Arrays.asList(key.split("\\."))
            : Arrays.asList("meta", key);
        if (f.getValueType() == Heka.Field.ValueType.STRING) {
          String value = f.getValueString(0);
          if (value.charAt(0) == '{') {
            insertPath(payload, path, new JSONObject(value));
          } else if (value.charAt(0) == '[') {
            insertPath(payload, path, new JSONArray(value));
          } else {
            insertPath(payload, path, value);
          }
        } else if (f.getValueType() == Heka.Field.ValueType.BOOL) {
          insertPath(payload, path, f.getValueBool(0));
        } else if (f.getValueType() == Heka.Field.ValueType.INTEGER) {
          insertPath(payload, path, f.getValueInteger(0));
        } else if (f.getValueType() == Heka.Field.ValueType.DOUBLE) {
          insertPath(payload, path, f.getValueDouble(0));
        }
        // FIXME: do we need to handle byte fields?
        // see code in moztelemetry and
        // https://bugzilla.mozilla.org/show_bug.cgi?id=1339421
      }
    }
    return payload;
  }

  public static List<JSONObject> readHekaStream(InputStream is) throws IOException {
    List<JSONObject> decodedMessages = new LinkedList<JSONObject>();

    while (true) {
      JSONObject o = readHekaMessage(is);
      if (o == null) {
        break;
      }
      decodedMessages.add(o);
    }
    return decodedMessages;
  }
}
