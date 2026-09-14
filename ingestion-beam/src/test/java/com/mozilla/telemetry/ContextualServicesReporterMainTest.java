package com.mozilla.telemetry;

import static org.hamcrest.MatcherAssert.assertThat;

import com.mozilla.telemetry.matchers.Lines;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Main class test that run the whole job via {@link ContextualServicesReporter#main} on the direct
 * runner, substituting local files for Pub/Sub and disabled reporting for the outbound HTTP call.
 */
public class ContextualServicesReporterMainTest {

  /**
   * Prefix that {@code SendRequest} uses when logging a reporting URL it has sent.
   */
  private static final String SENT_PREFIX = "Reporting URL sent: ";

  private static final String INPUT = "src/test/resources/contextualServices/"
      + "suggestReportingInput.ndjson";

  private static final String URL_ALLOW_LIST = "src/test/resources/contextualServices/"
      + "urlAllowlist.csv";

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();

  /**
   * With {@code --logReportingUrls=true} every URL that reaches {@code SendRequest} is written to
   * the error output, so the error output is the record of what the job would have requested.
   *
   * <p>The input holds two Mozilla-operated interactions and two AMP ones. Only the AMP pair may
   * appear. Asserting on the exact count also proves the Mozilla-operated pair was not rejected as
   * an allow list failure, which would have added two more rows.
   */
  @Test
  public void testOnlyAmpInteractionsAreReported() {
    String errorOutput = outputFolder.getRoot().getAbsolutePath() + "/error";

    ContextualServicesReporter.main(new String[] { //
        "--inputType=file", "--inputFileFormat=json", "--input=" + INPUT, //
        "--errorOutputType=file", "--errorOutput=" + errorOutput, //
        "--errorOutputFileCompression=UNCOMPRESSED", "--errorOutputNumShards=1", //
        "--includeStackTrace=false", //
        "--urlAllowList=" + URL_ALLOW_LIST, //
        "--allowedNamespaces=contextual-services", //
        "--allowedDocTypes=quicksuggest-impression,quicksuggest-click", //
        // Log the URLs that would be requested without actually requesting them.
        "--reportingEnabled=false", "--logReportingUrls=true" });

    List<String> sentUrls = reportedUrls(errorOutput);

    assertThat("only the two AMP interactions are reported", sentUrls, Matchers.hasSize(2));
    assertThat(sentUrls, Matchers.hasItem(Matchers.startsWith("https://imp.mt48.net/imp?")));
    assertThat(sentUrls,
        Matchers.hasItem(Matchers.startsWith("https://bridge.us.admarketplace.net/ctp?")));

    // Nothing identifying a Mozilla-operated interaction may leave the job.
    assertThat(sentUrls, Matchers.everyItem(Matchers.not(Matchers.containsString("mozilla.org"))));
    assertThat(sentUrls,
        Matchers.everyItem(Matchers.not(Matchers.containsString("suggestion_id"))));
  }

  /**
   * Extract the reporting URLs that {@code SendRequest} logged to the error output.
   */
  private static List<String> reportedUrls(String errorOutput) {
    return Lines.files(errorOutput + "*.ndjson").stream().map(line -> {
      try {
        return Json.readObjectNode(line).path("attributeMap").path("error_message").asText();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).filter(message -> message.contains(SENT_PREFIX))
        .map(message -> message.substring(message.indexOf(SENT_PREFIX) + SENT_PREFIX.length()))
        .collect(Collectors.toList());
  }
}
