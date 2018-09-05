/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.util.DurationUtils;
import com.mozilla.telemetry.transforms.JsonToPubsubMessage;
import com.mozilla.telemetry.transforms.PubsubMessageToTableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.codehaus.jackson.map.ObjectMapper;

public class Sink {
  /**
   * Options supported by {@link Sink}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("Type of --input; must be one of [pubsub, file]")
    @Default.Enum("pubsub")
    InputType getInputType();

    void setInputType(InputType value);

    @Description("File format for --inputType=file; must be one of"
        + " json (each line contains payload[String] and attributeMap[String,String]) or"
        + " text (each line is payload)")
    @Default.Enum("json")
    FileFormat getInputFileFormat();

    void setInputFileFormat(FileFormat value);

    @Description("Type of --output; must be one of [pubsub, file, stdout]")
    @Default.Enum("file")
    OutputType getOutputType();

    void setOutputType(OutputType value);

    @Description("File format for --outputType=file|stdout; must be one of "
        + " json (each line contains payload[String] and attributeMap[String,String]) or"
        + " text (each line is payload)")
    @Default.Enum("json")
    FileFormat getOutputFileFormat();

    void setOutputFileFormat(FileFormat value);

    @Description("Type of --errorOutput; must be one of [pubsub, file]")
    @Default.Enum("pubsub")
    ErrorOutputType getErrorOutputType();

    void setErrorOutputType(ErrorOutputType value);

    @Description("Fixed window duration. Defaults to 10m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("10m")
    String getWindowDuration();

    void setWindowDuration(String value);

    /* Note: Dataflow templates accept ValueProvider options at runtime, and
     * other options at creation time. When running without templates specify
     * all options at once.
     */

    @Description("Input to read from (path to file, PubSub subscription, etc.)")
    @Required
    ValueProvider<String> getInput();

    void setInput(ValueProvider<String> value);

    @Description("Output to write to (path to file or directory, Pubsub topic, etc.)")
    @Required
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);

    @Description("Error output to write to (path to file or directory, Pubsub topic, etc.)")
    @Required
    ValueProvider<String> getErrorOutput();

    void setErrorOutput(ValueProvider<String> value);
  }

  public enum FileFormat { json, text }

  public enum InputType { pubsub, file }

  public enum OutputType { pubsub, file, stdout, stderr, bigquery }

  public enum ErrorOutputType { pubsub, file, stdout, stderr }

  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    // register options class so that `--help=Options` works
    PipelineOptionsFactory.register(Options.class);

    final Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    final PTransform<PCollection<PubsubMessage>, PDone> errorOutput =
        getErrorOutputTransform(options);

    final Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("input", getInputTransform(options, errorOutput))
        .apply("output", getOutputTransform(options, errorOutput));
    pipeline.run();

  }

  /**
   * Generate an input transform from options and errorOutput.
   *
   * @param options provided to configure this pipeline.
   * @param errorOutput to which errors should be sent.
   * @return input transform.
   */
  public static PTransform<PBegin, PCollection<PubsubMessage>> getInputTransform(
      Options options, PTransform<PCollection<PubsubMessage>, PDone> errorOutput) {
    switch (options.getInputType()) {
      default: throw new IllegalArgumentException(
          "Unknown --inputType=" + options.getInputType().name());
      case pubsub:
        return PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput());
      case file: return new PTransform<PBegin, PCollection<PubsubMessage>>() {
        @Override
        public PCollection<PubsubMessage> expand(PBegin input) {
          final PCollection<String> lines = input.apply(TextIO.read().from(options.getInput()));
          switch (options.getInputFileFormat()) {
            default:
            case text:
              return lines.apply(decodeText);
            case json:
              PCollectionTuple result = lines.apply(decodeJson);
              result.get(decodeJson.errorTag).apply(errorOutput);
              return result.get(decodeJson.mainTag);
          }
        }
      };
    }
  }


  /**
   * Generate an output transform from options and errorOutput.
   *
   * @param options provided to configure this pipeline.
   * @param errorOutput to which errors should be sent.
   * @return output transform.
   */
  public static PTransform<PCollection<PubsubMessage>, PDone> getOutputTransform(
      Options options, PTransform<PCollection<PubsubMessage>, PDone> errorOutput) {
    MapElements<PubsubMessage, String> encode;
    switch (options.getOutputFileFormat()) {
      default: throw new IllegalArgumentException(
          "Unknown --outputFileFormat=" + options.getOutputFileFormat().name());
      case json:
        encode = encodeJson;
        break;
      case text:
        encode = encodeText;
        break;
    }
    switch (options.getOutputType()) {
      default: throw new IllegalArgumentException(
          "Unknown --outputType=" + options.getOutputType().name());
      case pubsub: return PubsubIO.writeMessages().to(options.getOutput());
      case file: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          input
              .apply(encode)
              .apply(Window.into(
                  FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
              .apply(TextIO.write().to(options.getOutput()).withWindowedWrites());
          return PDone.in(input.getPipeline());
        }
      };
      case bigquery: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          AsJsons<TableRow> encodeTableRow = AsJsons.of(TableRow.class);
          PCollectionTuple result = input.apply(decodeTableRow);
          result.get(decodeTableRow.errorTag).apply(errorOutput);
          result
              .get(decodeTableRow.mainTag)
              .apply(BigQueryIO
                  .writeTableRows()
                  .withMethod(Method.STREAMING_INSERTS)
                  .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                  .to(options.getOutput()))
              .getFailedInserts()
              .apply(encodeTableRow)
              .apply(decodeText) // TODO: add error_{type,message} fields
              .apply(errorOutput);
          return PDone.in(input.getPipeline());
        }
      };
      case stdout: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        class Fn extends DoFn<String, String> {
          @ProcessElement
          public void processElement(@Element String element) {
            System.out.println(element);
          }
        }

        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          input.apply(encodeJson).apply(ParDo.of(new Fn()));
          return PDone.in(input.getPipeline());
        }
      };
      case stderr: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        class Fn extends DoFn<String, String> {
          @ProcessElement
          public void processElement(@Element String element) {
            System.err.println(element);
          }
        }

        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          input.apply(encodeJson).apply(ParDo.of(new Fn()));
          return PDone.in(input.getPipeline());
        }
      };
    }
  }


  /**
   * Generate an output transform from options for delivering errors.
   *
   * <p>Only supports output methods that won't reject messages.
   *
   * @param options provided to configure this pipeline.
   * @return output transform.
   */
  public static PTransform<PCollection<PubsubMessage>, PDone> getErrorOutputTransform(
      Options options) {
    switch (options.getErrorOutputType()) {
      default: throw new IllegalArgumentException(
          "Unknown --errorOutputType=" + options.getErrorOutputType().name());
      case pubsub: return PubsubIO.writeMessages().to(options.getErrorOutput());
      case file: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          input
              .apply(encodeJson)
              .apply(Window.into(
                  FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
              .apply(TextIO
                  .write()
                  .to(options.getErrorOutput())
                  .withWindowedWrites());
          return PDone.in(input.getPipeline());
        }
      };
      case stdout: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        class Fn extends DoFn<String, String> {
          @ProcessElement
          public void processElement(@Element String element) {
            System.out.println(element);
          }
        }

        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          input.apply(encodeJson).apply(ParDo.of(new Fn()));
          return PDone.in(input.getPipeline());
        }
      };
      case stderr: return new PTransform<PCollection<PubsubMessage>, PDone>() {
        class Fn extends DoFn<String, String> {
          @ProcessElement
          public void processElement(@Element String element) {
            System.err.println(element);
          }
        }

        @Override
        public PDone expand(PCollection<PubsubMessage> input) {
          input.apply(encodeJson).apply(ParDo.of(new Fn()));
          return PDone.in(input.getPipeline());
        }
      };
    }
  }

  public static final MapElements<String, PubsubMessage> decodeText = MapElements
      .into(new TypeDescriptor<PubsubMessage>() { })
      .via((String value) -> new PubsubMessage(value.getBytes(), null));

  public static final MapElements<PubsubMessage, String> encodeText = MapElements
      .into(new TypeDescriptor<String>() { })
      .via((PubsubMessage value) -> new String(value.getPayload()));

  // TODO extract these into a separate class with error output
  public static final ObjectMapper objectMapper = new ObjectMapper();

  public static final MapElements<PubsubMessage, String> encodeJson = MapElements.via(
      new SimpleFunction<PubsubMessage, String>() {
        @Override
        public String apply(PubsubMessage value) {
          try {
            return objectMapper.writeValueAsString(value);
          } catch (Throwable e) {
            throw new RuntimeException();
          }
        }
      }
  );

  public static final JsonToPubsubMessage decodeJson = new JsonToPubsubMessage();

  public static final PubsubMessageToTableRow decodeTableRow = new PubsubMessageToTableRow();
}
