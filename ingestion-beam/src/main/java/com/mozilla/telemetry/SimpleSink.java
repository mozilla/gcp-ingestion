package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.util.Json;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSink {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleSink.class);

  /**
   * Execute an Apache Beam pipeline.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    run(args);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   *
   * @param args command line arguments
   */
  public static PipelineResult run(String[] args) {
    registerOptions();
    final SinkOptions.Parsed options = SinkOptions.parseSinkOptions(
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SinkOptions.class));

    return run(options);
  }

  /**
   * Execute an Apache Beam pipeline and return the {@code PipelineResult}.
   */
  public static PipelineResult run(SinkOptions.Parsed options) {
    final Pipeline pipeline = Pipeline.create(options);

    pipeline //
        .apply(options.getInputType().read(options)) //
        .apply(MapElements.into(TypeDescriptors.strings()).via(message -> {
          byte[] decompressedPayload;
          try {
            ByteArrayInputStream payloadStream = new ByteArrayInputStream(message.getPayload());
            GZIPInputStream gzipStream = new GZIPInputStream(payloadStream);
            ByteArrayOutputStream decompressedStream = new ByteArrayOutputStream();
            // Throws IOException
            IOUtils.copy(gzipStream, decompressedStream);
            decompressedPayload = decompressedStream.toByteArray();
          } catch (IOException e) {
            LOG.error("Exception raised while attempting to decompress payload with timestamp "
                + message.getAttribute("submission_timestamp") + " and observed length "
                + message.getPayload().length, e);
            decompressedPayload = message.getPayload();
          }
          final ObjectNode json;
          try {
            json = Json.readObjectNode(decompressedPayload);
          } catch (IOException e) {
            String b64 = Base64.getEncoder().encodeToString(decompressedPayload).substring(0, 80);
            throw new UncheckedIOException("Exception parsing payload with timestamp "
                + message.getAttribute("submission_timestamp") + " and observed length "
                + message.getPayload().length + " starting with content (base64-encoded): " + b64,
                e);
          }
          return json.toString();
        }));

    return pipeline.run();
  }

  /**
   * Register all options classes so that `--help=DecoderOptions`, etc.
   * works without having to specify additionally specify the appropriate mainClass.
   */
  static void registerOptions() {
    // register options classes so that `--help=SinkOptions`, etc. works
    PipelineOptionsFactory.register(SinkOptions.class);
    PipelineOptionsFactory.register(DecoderOptions.class);
  }

}
