/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.ingestion.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Time {

  public static java.time.Duration parseJavaDuration(String value) {
    checkNotNull(value, "The specified duration must be a non-null value!");
    java.time.Duration duration;

    try {
      // This is already an ISO-8601 duration.
      duration = java.time.Duration.parse(value);
    } catch (java.time.format.DateTimeParseException outer) {
      String modifiedValue = value.toLowerCase().replaceAll("seconds", "s")
          .replaceAll("second", "s").replaceAll("sec", "s").replaceAll("minutes", "m")
          .replaceAll("minute", "m").replaceAll("mins", "m").replaceAll("min", "m")
          .replaceAll("hours", "h").replaceAll("hour", "h").replaceAll("days", "dt")
          .replaceAll("day", "dt").replaceAll("\\s+", "").toUpperCase();
      if (!modifiedValue.contains("T")) {
        modifiedValue = "T" + modifiedValue;
      }
      if (!modifiedValue.contains("P")) {
        modifiedValue = "P" + modifiedValue;
      }
      if (modifiedValue.endsWith("T")) {
        modifiedValue += "0s";
      }
      try {
        duration = java.time.Duration.parse(modifiedValue);
      } catch (java.time.format.DateTimeParseException e) {
        throw new IllegalArgumentException(
            "User-provided duration '" + value + "' was transformed to '" + modifiedValue
                + "', but java.time.Duration.parse() could not understand it.",
            e);
      }
    }

    checkArgument(duration.toMillis() > 0, "The window duration must be greater than 0!");

    return duration;
  }
}
