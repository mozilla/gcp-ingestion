/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.options;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;

/**
 * Enum for the two types of inserts we can make to BigQuery, along with options that we need
 * to set differently for the two types; see https://cloud.google.com/bigquery/quotas
 */
public enum BigQueryWriteMethod {
  streaming(Method.STREAMING_INSERTS, 1000 * 1000), file_loads(Method.FILE_LOADS, 10 * 1000 * 1000);

  public final BigQueryIO.Write.Method method;
  public final int maxPayloadBytes;

  BigQueryWriteMethod(Method method, int maxPayloadBytes) {
    this.method = method;
    this.maxPayloadBytes = maxPayloadBytes;
  }
}
