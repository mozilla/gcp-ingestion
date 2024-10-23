package com.mozilla.telemetry.ingestion.sink;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class LoggingTest {

  @Rule
  public final LoggerContextRule logs = new LoggerContextRule("log4j2-list.yaml");

  @Test
  public void canWriteLogs() {
    final ListAppender appender = logs.getListAppender("STDOUT");

    // emit warning with exception from an async CompletableFuture
    CompletableFuture.runAsync(() -> LoggerFactory.getLogger(LoggingTest.class).warn("msg",
        new UncheckedIOException(new IOException("test"))), ForkJoinPool.commonPool()).join();
    assertThat(appender.getMessages(), containsInAnyOrder(allOf(containsString("LoggingTest"),
        containsString("\"level\":\"WARN\""), containsString("\"message\":\"msg\""), containsString(
            "\"extendedStackTrace\":\"java.io.UncheckedIOException: java.io.IOException: test"))));

    // emit error without exception from an async CompletableFuture
    appender.clear();
    LoggerFactory.getLogger(LoggingTest.class).error("test message");
    assertThat(appender.getMessages(),
        containsInAnyOrder(allOf(containsString("LoggingTest"),
            containsString("\"level\":\"ERROR\""), containsString("\"message\":\"test message\""),
            not(containsString("extendedStackTrace")))));
  }
}
