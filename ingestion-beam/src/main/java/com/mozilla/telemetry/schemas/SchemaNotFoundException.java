package com.mozilla.telemetry.schemas;

public class SchemaNotFoundException extends Exception {

  public SchemaNotFoundException(String message) {
    super(message);
  }

  public static SchemaNotFoundException forName(String name) {
    return new SchemaNotFoundException("No schema with name: " + name);
  }
}
