package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mozilla.telemetry.decoder.DecoderOptions;
import com.mozilla.telemetry.ingestion.core.util.Json;
import com.mozilla.telemetry.options.SinkOptions;
import com.mozilla.telemetry.transforms.DecompressPayload;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Sink {

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
    final List<PCollection<PubsubMessage>> failureCollections = new ArrayList<>();

    pipeline //
        .apply(options.getInputType().read(options)) //
        .apply(DecompressPayload.enabled(options.getDecompressInputPayloads())) //
        .apply(options.getOutputType().write(options)).failuresTo(failureCollections);

    PCollectionList.of(failureCollections) //
        .apply("FlattenFailureCollections", Flatten.pCollections()) //
        .apply("WriteErrorOutput", options.getErrorOutputType().write(options)) //
        .output();

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

  static class FlexTemplateMetadata {

    public static void main(String[] args) {
      run(SinkOptions.class);
    }

    static void run(Class<? extends PipelineOptions> optionsClass) {
      final ArrayNode parameters = Json.createArrayNode();
      for (Method method : optionsClass.getMethods()) {
        if (PipelineOptions.class.equals(method.getDeclaringClass())
            || !method.getName().startsWith("get")) {
          // skip methods from the base class and non-getter methods
          continue;
        }
        final String property = method.getName().substring(3);
        final String name = property.substring(0, 1).toLowerCase() + property.substring(1);
        final String label = property.replaceAll("([A-Z])", " $1").trim().replaceFirst("Bq", "BQ");
        final boolean isOptional = method.getAnnotation(Required.class) == null;
        final String description = method.getAnnotation(Description.class).value();

        final String regex;
        final String defaultValue;
        Class<?> returnType = method.getReturnType();
        if (returnType.isEnum()) {
          regex = Arrays.asList((Enum[]) returnType.getEnumConstants()).stream().map(Enum::name)
              .collect(Collectors.joining("|"));
          final Default.Enum defaultAnnotation = method.getAnnotation(Default.Enum.class);
          defaultValue = defaultAnnotation == null ? null : defaultAnnotation.value();
        } else if (int.class.equals(returnType) || Integer.class.equals(returnType)) {
          regex = "[0-9]+";
          final Default.Integer defaultAnnotation = method.getAnnotation(Default.Integer.class);
          defaultValue = defaultAnnotation == null ? null
              : Integer.toString(defaultAnnotation.value());
        } else if (long.class.equals(returnType) || Long.class.equals(returnType)) {
          regex = "[0-9]+";
          final Default.Long defaultAnnotation = method.getAnnotation(Default.Long.class);
          defaultValue = defaultAnnotation == null ? null
              : Long.toString(defaultAnnotation.value());
        } else {
          regex = ".*";
          final Default.String defaultAnnotation = method.getAnnotation(Default.String.class);
          defaultValue = defaultAnnotation == null ? null : defaultAnnotation.value();
        }

        parameters.add(Json.createObjectNode().put("name", name).put("label", label)
            .put("isOptional", isOptional)
            .put("helpText",
                description + (defaultValue == null ? "" : " Default: " + defaultValue))
            .set("regexes", Json.createArrayNode().add(regex)));
      }
      final String jobType = optionsClass.getSimpleName().replaceAll("Options", "");
      final JsonNode metadata = Json.createObjectNode()
          .put("name", "Mozilla Ingestion Beam " + jobType).set("parameters", parameters);
      System.out.println(Json.asString(metadata));
    }
  }
}
