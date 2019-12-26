package com.mozilla.telemetry.ingestion.sink.transform;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.pubsub.v1.PubsubMessage;
import com.mozilla.telemetry.ingestion.core.Constant.Attribute;
import com.mozilla.telemetry.ingestion.core.util.DerivedAttributesMap;
import com.mozilla.telemetry.ingestion.core.util.SnakeCase;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.commons.text.StringSubstitutor;

/**
 * Transform a {@link PubsubMessage} into a {@link String} using {@link StringSubstitutor}.
 *
 * <p>All message attributes, plus derived attributes provided by {@link DerivedAttributesMap},
 * are available for substitution.
 *
 * @throws IllegalArgumentException if template is not fully resolved for a given message.
 */
public class PubsubMessageToTemplatedString implements Function<PubsubMessage, String> {

  public final String template;

  public static PubsubMessageToTemplatedString of(String template) {
    return new PubsubMessageToTemplatedString(template);
  }

  public static PubsubMessageToTemplatedString forBigQuery(String template) {
    return new PubsubMessageToTemplatedStringForBigQuery(template);
  }

  @Override
  public String apply(PubsubMessage message) {
    final String batchKey = StringSubstitutor.replace(template,
        DerivedAttributesMap.of(message.getAttributesMap()));
    if (batchKey.contains("$")) {
      throw new IllegalArgumentException("Element did not contain all the attributes needed to"
          + " fill out variables in the configured template: " + template);
    }
    return batchKey;
  }

  ////

  private PubsubMessageToTemplatedString(String template) {
    this.template = template;
  }

  private static class PubsubMessageToTemplatedStringForBigQuery
      extends PubsubMessageToTemplatedString {

    private transient Cache<String, String> normalizedNameCache;

    private PubsubMessageToTemplatedStringForBigQuery(String template) {
      super(template);
      this.normalizedNameCache = CacheBuilder.newBuilder().maximumSize(50_000).build();
    }

    private String getAndCacheNormalizedName(String name) {
      try {
        return normalizedNameCache.get(name, () -> SnakeCase.format(name));
      } catch (ExecutionException e) {
        throw new UncheckedExecutionException(e.getCause());
      }
    }

    @Override
    public String apply(PubsubMessage message) {
      Map<String, String> attributes = new HashMap<>(message.getAttributesMap());

      // We coerce all docType and namespace names to be snake_case and to remove invalid
      // characters; these transformations MUST match with the transformations applied by the
      // jsonschema-transpiler and mozilla-schema-generator when creating table schemas in BigQuery.
      final String namespace = attributes.get(Attribute.DOCUMENT_NAMESPACE);
      final String docType = attributes.get(Attribute.DOCUMENT_TYPE);
      if (namespace != null) {
        attributes.put(Attribute.DOCUMENT_NAMESPACE, getAndCacheNormalizedName(namespace));
      }
      if (docType != null) {
        attributes.put(Attribute.DOCUMENT_TYPE, getAndCacheNormalizedName(docType));
      }

      attributes = Maps.transformValues(DerivedAttributesMap.of(attributes),
          v -> v.replaceAll("-", "_"));

      return super.apply(PubsubMessage.newBuilder().putAllAttributes(attributes).build());
    }
  }
}
