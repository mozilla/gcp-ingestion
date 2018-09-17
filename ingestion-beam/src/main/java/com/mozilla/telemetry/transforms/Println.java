/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Transform that prints each element of the collection to a specified console stream.
 */
public class Println extends PTransform<PCollection<String>, PDone> {
  private final DoFn<String, Void> fn;

  // Private constructor forces use of static factory methods.
  private Println(DoFn<String, Void> fn) {
    this.fn = fn;
  }

  /** Transform that prints to stdout. */
  public static Println stdout() {
    return new Println(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(@Element String element) {
        System.out.println(element);
      }
    });
  }

  /** Transform that prints to stderr. */
  public static Println stderr() {
    return new Println(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(@Element String element) {
        System.err.println(element);
      }
    });
  }

  @Override
  public PDone expand(PCollection<String> input) {
    input.apply(ParDo.of(fn));
    return PDone.in(input.getPipeline());
  }
}
