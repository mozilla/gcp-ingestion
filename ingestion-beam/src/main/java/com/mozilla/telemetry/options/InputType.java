package com.mozilla.telemetry.options;

import com.mozilla.telemetry.io.Read;
import com.mozilla.telemetry.io.Read.BigQueryInput;
import com.mozilla.telemetry.io.Read.FileInput;
import com.mozilla.telemetry.io.Read.PubsubInput;

public enum InputType {

  pubsub {

    /** Return a PTransform that reads from a Pubsub subscription. */
    public Read read(SinkOptions.Parsed options) {
      return new PubsubInput(options.getInput());
    }
  },

  file {

    /** Return a PTransform that reads from local or remote files. */
    public Read read(SinkOptions.Parsed options) {
      return new FileInput(options.getInput(), options.getInputFileFormat());
    }
  },

  bigquery_table {

    /** Return a PTransform that reads from a BigQuery table. */
    public Read read(SinkOptions.Parsed options) {
      return new BigQueryInput(options.getInput(), options.getBqReadMethod(),
          BigQueryInput.Source.TABLE, options.getBqRowRestriction(), options.getBqSelectedFields());
    }

  },

  bigquery_query {

    /** Return a PTransform that reads the results of a BigQuery query. */
    public Read read(SinkOptions.Parsed options) {
      return new BigQueryInput(options.getInput(), options.getBqReadMethod(),
          BigQueryInput.Source.QUERY, options.getBqRowRestriction(), options.getBqSelectedFields());
    }

  };

  public abstract Read read(SinkOptions.Parsed options);
}
