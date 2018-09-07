/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.mozilla.telemetry.transforms.FailureMessage;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class ParseUri
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  public static TupleTag<PubsubMessage> mainTag = new TupleTag<PubsubMessage>();
  public static TupleTag<PubsubMessage> errorTag = new TupleTag<PubsubMessage>();
  public static TupleTagList additionalOutputTags = TupleTagList.of(errorTag);

  private static Map<String, String> zip(String[] keys, String[] values) {
    Map<String, String> map = new HashMap<>();
    int length = Math.min(keys.length, values.length);
    for (int i = 0; i < length; i++) {
      map.put(keys[i], values[i]);
    }
    return map;
  }

  private static final String TELEMETRY_URI_PREFIX = "/submit/telemetry/";
  private static final String[] TELEMETRY_URI_SUFFIX_ELEMENTS = new String[]{
      "document_id", "document_type", "app_name", "app_version", "app_update_channel",
      "app_build_id"};
  private static final String GENERIC_URI_PREFIX = "/submit/";
  private static final String[] GENERIC_URI_SUFFIX_ELEMENTS = new String[]{
      "document_namespace", "document_type", "document_version", "document_id"};

  private static PubsubMessage transform(PubsubMessage value) {
    // Copy attributes
    final Map<String, String> attributes = new HashMap<>(value.getAttributeMap());

    // parse uri based on prefix
    final String uri = attributes.get("uri");
    if (uri.startsWith(TELEMETRY_URI_PREFIX)) {
      // TODO acquire document_version from attributes.get("args")
      attributes.put("document_namespace", "telemetry");
      attributes.putAll(zip(
          TELEMETRY_URI_SUFFIX_ELEMENTS,
          uri.substring(TELEMETRY_URI_PREFIX.length()).split("/")));
    } else if (uri.startsWith(GENERIC_URI_PREFIX)) {
      attributes.putAll(zip(
          GENERIC_URI_SUFFIX_ELEMENTS,
          uri.substring(GENERIC_URI_PREFIX.length()).split("/")));
    } else {
      throw new AssertionError();
    }
    attributes.remove("uri");
    return new PubsubMessage(value.getPayload(), attributes);
  }

  private static class Fn extends DoFn<PubsubMessage, PubsubMessage> {
    @ProcessElement
    public void processElement(@Element PubsubMessage element, MultiOutputReceiver out) {
      try {
        out.get(mainTag).output(transform(element));
      } catch (Throwable e) {
        out.get(errorTag).output(
            FailureMessage.of(ParseUri.class.getName(), element, e));
      }
    }
  }

  private static final Fn FN = new Fn();

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple output = input.apply(ParDo
        .of(FN)
        .withOutputTags(mainTag, additionalOutputTags)
    );
    output.get(errorTag).setCoder(PubsubMessageWithAttributesCoder.of());
    return output;
  }
}
