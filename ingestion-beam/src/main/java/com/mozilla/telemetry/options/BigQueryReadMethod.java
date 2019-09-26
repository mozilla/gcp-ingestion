/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

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
