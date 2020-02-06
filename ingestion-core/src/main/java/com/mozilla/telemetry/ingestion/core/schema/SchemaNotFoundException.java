package com.mozilla.telemetry.ingestion.core.schema;

public class SchemaNotFoundException extends RuntimeException {

  public SchemaNotFoundException(String message) {
    super(message);
  }

  public static SchemaNotFoundException forName(String name) {
    return new SchemaNotFoundException("No schema with name: " + name);
  }
}
