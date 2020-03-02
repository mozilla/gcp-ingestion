package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Test if a {@link PubsubMessage} has a document type that matches a {@link Pattern}.
 *
 * <p>{@link Pattern} must match "${document_namespace}/${document_type}" normalized to kebab-case.
 *
 * <p>Uses {@link Cache} to cache results.
 *
 * <p>Multiple document types may be matched using {@code "|"}, and a whole namespace may be matched
 * using {@code "namespace/.*"}, for example:
 * {@code Pattern.compile("activity-stream/impression-stats|messaging-system/.*")}
 */
public class DocumentTypePredicate implements Predicate<PubsubMessage> {

  private final Cache<String, Boolean> cache = CacheBuilder.newBuilder().maximumSize(50_000)
      .build();
  private final PubsubMessageToTemplatedString template = PubsubMessageToTemplatedString
      .forBigQuery("${" + Attribute.DOCUMENT_NAMESPACE + "}/${" + Attribute.DOCUMENT_TYPE + "}");
  private final Pattern pattern;

  private DocumentTypePredicate(Pattern pattern) {
    this.pattern = pattern;
  }

  public static DocumentTypePredicate of(Pattern pattern) {
    return new DocumentTypePredicate(pattern);
  }

  @Override
  public boolean test(PubsubMessage message) {
    String doctype = template.apply(message);
    try {
      return cache.get(doctype, () -> pattern.matcher(doctype.replaceAll("_", "-")).matches());
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e.getCause());
    }
  }
}
