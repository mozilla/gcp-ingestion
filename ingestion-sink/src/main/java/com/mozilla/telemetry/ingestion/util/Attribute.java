/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.util;

public class Attribute {

  private Attribute() {
  }

  public static final String APP_BUILD_ID = "app_build_id";
  public static final String APP_NAME = "app_name";
  public static final String APP_UPDATE_CHANNEL = "app_update_channel";
  public static final String APP_VERSION = "app_version";
  public static final String CLIENT_ID = "client_id";
  public static final String DATE = "date";
  public static final String DNT = "dnt";
  public static final String DOCUMENT_ID = "document_id";
  public static final String DOCUMENT_NAMESPACE = "document_namespace";
  public static final String DOCUMENT_TYPE = "document_type";
  public static final String DOCUMENT_VERSION = "document_version";
  public static final String NORMALIZED_APP_NAME = "normalized_app_name";
  public static final String NORMALIZED_CHANNEL = "normalized_channel";
  public static final String NORMALIZED_COUNTRY_CODE = "normalized_country_code";
  public static final String NORMALIZED_OS = "normalized_os";
  public static final String NORMALIZED_OS_VERSION = "normalized_os_version";
  public static final String SAMPLE_ID = "sample_id";
  public static final String SUBMISSION_TIMESTAMP = "submission_timestamp";
  public static final String URI = "uri";
  public static final String USER_AGENT = "user_agent";
  public static final String X_DEBUG_ID = "x_debug_id";
  public static final String X_PINGSENDER_VERSION = "x_pingsender_version";
}
