package com.mozilla.telemetry.ingestion.core.util;

import java.io.IOException;

@FunctionalInterface
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface IOFunction<T, R> {

  R apply(T input) throws IOException;
}
