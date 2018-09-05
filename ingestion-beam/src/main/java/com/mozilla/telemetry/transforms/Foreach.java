/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import java.util.function.Consumer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * A PTransform for running a function with side effects, but no output.
 *
 * <p>For now, only provides a static {@code string} method for input collections of strings.
 *
 * <p>It is intended to be used inside a ParDo with a Java 8 lambda expression like
 * {@code input.apply(Foreach.string(System.out::println))}
 *
 * @param <InputT> Input collection type
 */
public class Foreach<InputT> extends PTransform<PCollection<InputT>, PDone> {

  /**
   * Return a PTransform applying fn to the input collection of Strings.
   */
  public static Foreach<String> string(Consumer<String> fn) {
    return new Foreach<>((String record) -> {
      fn.accept(record);
      return null;
    });
  }

  private final SerializableFunction<InputT, Void> fn;

  private Foreach(SerializableFunction<InputT, Void> fn) {
    this.fn = fn;
  }

  class Fn extends DoFn<InputT, Void> {
    @ProcessElement
    public void processElement(@Element InputT element) {
      fn.apply(element);
    }
  }

  @Override
  public PDone expand(PCollection<InputT> input) {
    input.apply(ParDo.of(new Fn()));
    return PDone.in(input.getPipeline());
  }

}
