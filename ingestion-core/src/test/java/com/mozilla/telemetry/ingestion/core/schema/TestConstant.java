package com.mozilla.telemetry.ingestion.core.schema;

import com.google.common.io.Resources;

public class TestConstant {

  // Built by maven from src/test/resources/schema/test-schemas
  public static final String SCHEMAS_LOCATION = Resources.getResource("schema/test-schemas.tar.gz")
      .getPath();
}
