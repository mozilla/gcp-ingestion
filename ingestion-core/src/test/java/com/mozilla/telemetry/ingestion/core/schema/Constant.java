package com.mozilla.telemetry.ingestion.core.schema;

import com.google.common.io.Resources;

class Constant {

  static final String SCHEMA_ALIASES_LOCATION = Resources
      .getResource("schema/example-aliasing-config.json").getPath();

  // Built by maven from src/test/resources/schema/test-schemas
  static final String SCHEMAS_LOCATION = Resources.getResource("schema/test-schemas.tar.gz")
      .getPath();
}
