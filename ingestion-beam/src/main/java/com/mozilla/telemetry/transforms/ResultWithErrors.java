/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * An intermediate output type for PTransforms that allows an output collection to live alongside
 * a collection of elements that failed the transform.
 *
 * <p>The error elements are always expected to be expressed as PubsubMessages, but the output
 * can be a PCollection of any type or another POutput implementation such as PDone.
 *
 * @param <T> Output collection type
 */
@AutoValue
public abstract class ResultWithErrors<T extends POutput> implements POutput, PInput {

  public abstract T output();

  @Nullable
  abstract TupleTag<?> outputTag();

  public abstract PCollection<PubsubMessage> errors();

  abstract TupleTag<PubsubMessage> errorTag();

  /**
   * Create a ResultWithErrors, specifying TupleTags for the success and error collections.
   * Use this form when your output is a PCollection.
   */
  public static <T extends PValue> ResultWithErrors<T> of(T output, @Nullable TupleTag<?> outputTag,
      PCollection<PubsubMessage> errors, TupleTag<PubsubMessage> errorTag) {
    return new AutoValue_ResultWithErrors<>(output, outputTag, errors, errorTag);
  }

  /**
   * Create a ResultWithErrors without specifying TupleTags.
   * Use this form when your output is not a PCollection.
   */
  public static <T extends POutput> ResultWithErrors<T> of(T successes,
      PCollection<PubsubMessage> errors) {
    TupleTag<PubsubMessage> errorTag = new TupleTag<>();
    return new AutoValue_ResultWithErrors<>(successes, null, errors, errorTag);
  }

  /**
   * Add this object's error collection to the given list, returning this object unchanged.
   * Useful for fluent-style error handling with chained method calls.
   */
  public ResultWithErrors<T> addErrorCollectionTo(
      List<PCollection<PubsubMessage>> errorCollections) {
    errorCollections.add(errors());
    return this;
  }

  @Override
  public Pipeline getPipeline() {
    return output().getPipeline();
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    Map<TupleTag<?>, PValue> values = new HashMap<>();
    values.put(errorTag(), errors());
    if (outputTag() != null && output() instanceof PValue) {
      values.put(outputTag(), (PValue) output());
    }
    return values;
  }

  @Override
  public void finishSpecifyingOutput(String transformName, PInput input,
      PTransform<?, ?> transform) {
    errors().setCoder(PubsubMessageWithAttributesCoder.of());
  }
}
