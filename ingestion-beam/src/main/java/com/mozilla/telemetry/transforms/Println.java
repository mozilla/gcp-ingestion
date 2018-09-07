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

  public enum Destination {
    stdout, stderr
  }

  private final Fn fn;

  // Private constructor forces use of static factory methods.
  private Println(Fn fn) {
    this.fn = fn;
  }

  public static Println to(Destination destination) {
    return new Println(new Fn(destination));
  }

  public static Println stdout() {
    return new Println(new Fn(Destination.stdout));
  }

  public static Println stderr() {
    return new Println(new Fn(Destination.stderr));
  }

  private static class Fn extends DoFn<String, Void> {
    private final Destination destination;

    public Fn(Destination destination) {
      this.destination = destination;
    }

    @ProcessElement
    public void processElement(@Element String element) {
      if (destination.equals(Destination.stdout)) {
        System.out.println(element);
      }
      if (destination.equals(Destination.stderr)) {
        System.err.println(element);
      }
    }
  }

  @Override
  public PDone expand(PCollection<String> input) {
    input.apply(
        "Print to " + fn.destination.toString(),
        ParDo.of(fn));
    return PDone.in(input.getPipeline());
  }
}
