package com.mozilla.telemetry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

public class PioneerBenchmarkGenerator {

  // create a temporary directory
  // generate a keyfile metadata
  // encode all of the messages
  // upload into a temporary directory (or named directory)
  final static ObjectMapper mapper = new ObjectMapper();

  public static Optional<String> transform(String data) {
    try {
      JsonNode node = mapper.readTree(data);
      return Optional.of(node.get("attributeMap").get("document_namespace").toString());
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public static void main(final String[] args) {
    try (Stream<String> stream = Files.lines(Paths.get("document_sample.ndjson"))) {

      Files.write(Paths.get("output.ndjson"), (Iterable<String>) stream.map(s -> transform(s))
          .filter(Optional::isPresent).map(Optional::get)::iterator);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}