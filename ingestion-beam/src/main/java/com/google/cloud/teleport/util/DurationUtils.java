/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Copied from GCP's DataflowTemplates repo (which is not published as an artifact):
// https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/304bdbf4a3e2eac78adc4b06f466da82b656ef2c/src/main/java/com/google/cloud/teleport/util/DurationUtils.java

package com.google.cloud.teleport.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Locale;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.MutablePeriod;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

/**
 * The {@link DurationUtils} class provides common utilities for manipulating and formatting {@link
 * Duration} objects.
 */
public class DurationUtils {

  /**
   * Parses a duration from a period formatted string. Values are accepted in the following formats:
   *
   * <p>Formats Ns - Seconds. Example: 5s<br>
   * Nm - Minutes. Example: 13m<br>
   * Nh - Hours. Example: 2h
   *
   * <pre>
   * parseDuration(null) = NullPointerException()
   * parseDuration("")   = Duration.standardSeconds(0)
   * parseDuration("2s") = Duration.standardSeconds(2)
   * parseDuration("5m") = Duration.standardMinutes(5)
   * parseDuration("3h") = Duration.standardHours(3)
   * </pre>
   *
   * @param value The period value to parse.
   * @return The {@link Duration} parsed from the supplied period string.
   */
  public static Duration parseDuration(String value) {
    checkNotNull(value, "The specified duration must be a non-null value!");

    PeriodParser parser =
        new PeriodFormatterBuilder()
            .appendSeconds()
            .appendSuffix("s")
            .appendMinutes()
            .appendSuffix("m")
            .appendHours()
            .appendSuffix("h")
            .toParser();

    MutablePeriod period = new MutablePeriod();
    parser.parseInto(period, value, 0, Locale.getDefault());

    Duration duration = period.toDurationFrom(new DateTime(0));
    checkArgument(duration.getMillis() > 0, "The window duration must be greater than 0!");

    return duration;
  }

  /**
   * Like {@link #parseDuration(String)}, but using a {@link ValueProvider}.
   *
   * <p>The value will be parsed once when first used; the result is cached and reused.
   */
  public static ValueProvider<Duration> parseDuration(ValueProvider<String> value) {
    return NestedValueProvider.of(value, DurationUtils::parseDuration);
  }
}
