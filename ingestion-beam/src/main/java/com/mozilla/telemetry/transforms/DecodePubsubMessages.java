/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * Base class for decoding PubsubMessage from various input formats.
 *
 * <p>Subclasses are provided via static members fromText() and fromJson().
 * We also provide an alreadyDecoded() transform for creating a PCollectionTuple
 * analogous to fromText or fromJson to handle output from PubsubIO.
 *
 * <p>The packaging of subclasses here follows the style guidelines as captured in
 * https://beam.apache.org/contribute/ptransform-style-guide/#packaging-a-family-of-transforms
 */
public abstract class DecodePubsubMessages
    extends MapElementsWithErrors.ToPubsubMessageFrom<String> {

  // Force use of static factory methods.
  private DecodePubsubMessages() {}

  /*
   * Static factory methods.
   */

  /** Decoder from non-json text to PubsubMessage. */
  public static Text text() {
    return new Text();
  }

  /** Decoder from json to PubsubMessage. */
  public static Json json() {
    return new Json();
  }


  /*
   * Concrete subclasses.
   */

  public static class Text extends DecodePubsubMessages {
    protected PubsubMessage processElement(String element) {
      return new PubsubMessage(element.getBytes(), null);
    }
  }

  public static class Json extends DecodePubsubMessages {
    protected PubsubMessage processElement(String element) throws IOException {
      return com.mozilla.telemetry.utils.Json.readPubsubMessage(element);
    }
  }
}
