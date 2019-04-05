/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * A transform that passes elements through unchanged; it only exists in order to publish
 * some bundle-level metrics.
 *
 * <p>The technique used here for storing state as members directly on the DoFn is likely not
 * entirely robust but probably good enough for our purposes based on the Beam Programming Guide's
 * claim that "each instance of your function object is accessed by a single thread at a time on a
 * worker instance." A more robust solution would be to make this a stateful DoFn so that Beam
 * has knowledge of the state, but that has the potential side effect of forcing additional
 * grouping operations under the hood.
 *
 * <p>We will likely remove this transform after we get a feeling for average bundle sizes
 * and whether bundles tend to be large enough that we could pursue a GCS sink that operates
 * at the bundle level; see https://github.com/mozilla/gcp-ingestion/issues/501
 */
public class PublishBundleMetrics
    extends PTransform<PCollection<? extends PubsubMessage>, PCollection<PubsubMessage>> {

  public static PublishBundleMetrics of() {
    return new PublishBundleMetrics();
  }

  private PublishBundleMetrics() {
  }

  private final Distribution bundleElements = Metrics.distribution(PublishBundleMetrics.class,
      "bundle_elements");
  private final Distribution bundleBytes = Metrics.distribution(PublishBundleMetrics.class,
      "bundle_bytes");

  private class Fn extends DoFn<PubsubMessage, PubsubMessage> {

    private long elements = 0;
    private long bytes = 0;

    @ProcessElement
    public void processElement(@Element PubsubMessage element, OutputReceiver<PubsubMessage> out) {
      out.output(element);
      elements += 1;
      bytes += element.getPayload().length;
    }

    @FinishBundle
    public void finishBundle() {
      bundleElements.update(elements);
      bundleBytes.update(bytes);

      elements = 0;
      bytes = 0;
    }

  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<? extends PubsubMessage> input) {
    return input.apply(ParDo.of(new Fn()));
  }
}
