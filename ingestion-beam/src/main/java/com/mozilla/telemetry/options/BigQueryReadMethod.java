package com.mozilla.telemetry.options;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;

public enum BigQueryReadMethod {
  export(Method.EXPORT), //
  storageapi(Method.DIRECT_READ);

  public final BigQueryIO.TypedRead.Method method;

  BigQueryReadMethod(BigQueryIO.TypedRead.Method method) {
    this.method = method;
  }
}
