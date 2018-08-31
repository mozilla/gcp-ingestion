/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry;

import com.google.api.services.bigquery.model.TableRow;
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
import org.joda.time.Duration;

public class Sink {
    /**
     * Options supported by {@link Sink}
     *
     * <p>Inherits standard configuration options.
     */
    private interface Options extends PipelineOptions {
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

        @Description("Fixed window duration, in minutes")
        @Default.Long(10)
        Long getWindowMinutes();
        void setWindowMinutes(Long value);

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

    enum FileFormat {
        json, text
    }

    enum InputType {
        pubsub, file
    }

    enum OutputType {
        pubsub, file, stdout, bigquery
    }

    enum ErrorOutputType {
        pubsub, file, stderr
    }

    public static void main(String[] args) {
        // register options class so that `--help=Options` works
        PipelineOptionsFactory.register(Options.class);

        Options options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(Options.class);

        PTransform<PCollection<PubsubMessage>, PDone> errorOutput = getErrorOutputTransform(options);
        PTransform<PCollection<PubsubMessage>, PDone> output = getOutputTransform(options, errorOutput);
        PTransform<PBegin, PCollection<PubsubMessage>> input = getInputTransform(options, errorOutput);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(input).apply(output);
        pipeline.run();

    }

    static PTransform<PBegin, PCollection<PubsubMessage>> getInputTransform(Options options, PTransform<PCollection<PubsubMessage>, PDone> errorOutput) {
        switch (options.getInputType()) {
            default:
            case pubsub: return PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInput());
            case file: return new PTransform<PBegin, PCollection<PubsubMessage>>() {
                @Override
                public PCollection<PubsubMessage> expand(PBegin input) {
                    PCollection<String> lines = input.apply(TextIO.read().from(options.getInput()));
                    switch(options.getInputFileFormat()) {
                        default:
                        case text: return lines.apply(decodeText);
                        case json:
                            PCollectionTuple result = lines.apply(decodeJson);
                            result.get(decodeJson.getErrorTag()).apply(errorOutput);
                            return result.get(decodeJson.getMainTag());
                    }
                }
            };
        }
    }

    static PTransform<PCollection<PubsubMessage>, PDone> getOutputTransform(Options options, PTransform<PCollection<PubsubMessage>, PDone> errorOutput) {
        MapElements<PubsubMessage, String> encode;
        switch (options.getOutputFileFormat()) {
            default:
            case json:
                encode = encodeJson;
                break;
            case text:
                encode = encodeText;
                break;
        }
        switch (options.getOutputType()) {
            default:
            case pubsub: return PubsubIO.writeMessages().to(options.getOutput());
            case file: return new PTransform<PCollection<PubsubMessage>, PDone>() {
                @Override
                public PDone expand(PCollection<PubsubMessage> input) {
                    input
                        .apply(encode)
                        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowMinutes()))))
                        .apply(TextIO.write().to(options.getOutput()).withWindowedWrites());
                    return PDone.in(input.getPipeline());
                }
            };
            case bigquery: return new PTransform<PCollection<PubsubMessage>, PDone>() {
                @Override
                public PDone expand(PCollection<PubsubMessage> input) {
                    AsJsons<TableRow> encodeTableRow = AsJsons.of(TableRow.class);
                    PCollectionTuple result = input.apply(decodeTableRow);
                    result.get(decodeTableRow.getErrorTag()).apply(errorOutput);
                    result
                        .get(decodeTableRow.getMainTag())
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
                    input.apply(encode).apply(ParDo.of(new Fn()));
                    return PDone.in(input.getPipeline());
                }
            };
        }
    }

    static PTransform<PCollection<PubsubMessage>, PDone> getErrorOutputTransform(Options options) {
        switch (options.getErrorOutputType()) {
            default:
            case pubsub: return PubsubIO.writeMessages().to(options.getErrorOutput());
            case file: return new PTransform<PCollection<PubsubMessage>, PDone>() {
                @Override
                public PDone expand(PCollection<PubsubMessage> input) {
                    input
                        .apply(encodeJson)
                        .apply(Window
                            .into(FixedWindows
                                .of(Duration
                                    .standardMinutes(options.getWindowMinutes()))))
                        .apply(TextIO
                            .write()
                            .to(options.getErrorOutput())
                            .withWindowedWrites());
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

    static MapElements<String, PubsubMessage> decodeText = MapElements
        .into(new TypeDescriptor<PubsubMessage>() { })
        .via((String value) -> new PubsubMessage(value.getBytes(), null));

    static MapElements<PubsubMessage, String> encodeText = MapElements
            .into(new TypeDescriptor<String>() { })
            .via((PubsubMessage value) -> new String(value.getPayload()));

    // TODO extract this into a separate class with error output
    static ObjectMapper objectMapper = new ObjectMapper();
    static MapElements<PubsubMessage, String> encodeJson = MapElements.via(
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

    static JsonToPubsubMessage decodeJson = new JsonToPubsubMessage();

    static PubsubMessageToTableRow decodeTableRow = new PubsubMessageToTableRow();
}
