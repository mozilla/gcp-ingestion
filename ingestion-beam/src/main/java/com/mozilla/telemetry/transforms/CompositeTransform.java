/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A factory for more easily creating custom PTransforms using Java 8 lambdas.
 *
 * <p>For example:
 *
 * {@code
 * CompositeTransform.of(input -> input
 *     .apply(PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput()))
 *     .apply(DecodePubsubMessages.AlreadyDecoded));
 * }
 * @param <InputT> Type of the input collection
 * @param <OutputT> Type of the output collection
 */
public interface CompositeTransform<InputT extends PInput, OutputT extends POutput> {

  OutputT expand(InputT input);

  /**
   * The public factory method that serves as the entrypoint for users to create a PTransform.
   */
  static <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> of(
      CompositeTransform<InputT, OutputT> transform) {
    return new PTransform<InputT, OutputT>() {

      @Override
      public OutputT expand(InputT input) {
        return transform.expand(input);
      }
    };
  }
}
