package com.mozilla.telemetry.ingestion.core.schema;

import com.google.common.io.Resources;

class Constant {

  static final String SCHEMA_ALIASES_LOCATION = Resources
      .getResource("schema/example-aliasing-config.json").getPath();

  /**
   * test-schema
   * └── schemas
   *     ├── namespace_0
   *     │   ├── foo
   *     │   │   ├── foo.1.avro.json
   *     │   │   └── foo.1.schema.json
   *     │   └── bar
   *     │       ├── bar.1.avro.json
   *     │       └── bar.1.schema.json
   *     └── namespace_1
   *         └── baz
   *             ├── baz.1.avro.json
   *             └── baz.1.schema.json
   * 7 directories, 6 files
   */
  static final String SCHEMAS_LOCATION = Resources.getResource("schema/test-schemas.tar.gz")
      .getPath();
}
