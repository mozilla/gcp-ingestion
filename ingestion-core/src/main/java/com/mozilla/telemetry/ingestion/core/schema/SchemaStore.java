package com.mozilla.telemetry.ingestion.core.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.ingestion.core.util.IOFunction;
import com.mozilla.telemetry.ingestion.core.util.SnakeCase;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.text.StringSubstitutor;

/** Reads schemas from a file and parses them into a map. */
public abstract class SchemaStore<T> {

  /**
   * Return the parsed schema corresponding to a path underneath the schemas/ directory.
   *
   * @throws SchemaNotFoundException if schema matching the attributes exists
   */
  public T getSchema(String path) throws SchemaNotFoundException {
    ensureSchemasLoaded();
    T schema = schemas.get(getAndCacheNormalizedPath(path));
    if (schema == null) {
      throw SchemaNotFoundException.forName(path);
    }
    return schema;
  }

  /**
   * Return the parsed schema corresponding to doctype and namespace parsed from attributes.
   *
   * @throws SchemaNotFoundException if schema matching the attributes exists
   */
  public T getSchema(Map<String, String> attributes) throws SchemaNotFoundException {
    ensureSchemasLoaded();
    if (attributes == null) {
      throw new SchemaNotFoundException("No schema for message with null attributeMap");
    }
    // This is the path provided by mozilla-pipeline-schemas
    final String path = StringSubstitutor.replace(
        "${document_namespace}/${document_type}/${document_type}.${document_version}", attributes)
        + schemaSuffix();
    return getSchema(path);
  }

  /** Returns true if we found at least one schema with the given doctype and namespace. */
  public boolean docTypeExists(String namespace, String docType) {
    ensureSchemasLoaded();
    if (namespace == null || docType == null) {
      return false;
    } else {
      return dirs.contains(
          getAndCacheNormalizedName(namespace) + "/" + getAndCacheNormalizedName(docType));
    }
  }

  /** Returns true if we found at least one schema with matching the given attributes. */
  public boolean docTypeExists(Map<String, String> attributes) {
    String namespace = getAndCacheNormalizedName(attributes.get("document_namespace"));
    String docType = getAndCacheNormalizedName(attributes.get("document_type"));
    return docTypeExists(namespace, docType);
  }

  ////////

  protected SchemaStore(String schemasLocation, IOFunction<String, InputStream> open) {
    this.schemasLocation = schemasLocation;
    if (open == null) {
      this.open = FileInputStream::new;
    } else {
      this.open = open;
    }
  }

  private final String schemasLocation;
  private final IOFunction<String, InputStream> open;

  /* Returns the expected suffix for a particular schema type e.g. `.schema.json` */
  protected abstract String schemaSuffix();

  /* Returns a parsed schema from an archive input stream. */
  protected abstract T loadSchemaFromArchive(ArchiveInputStream archive) throws IOException;

  ////////

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SchemaStore.class);
  private transient Map<String, T> schemas;
  private transient Set<String> dirs;
  private transient Cache<String, String> normalizedNameCache;

  @VisibleForTesting
  int numLoadedSchemas() {
    ensureSchemasLoaded();
    return schemas.size();
  }

  private void loadAllSchemas() throws IOException {
    final Map<String, T> tempSchemas = new HashMap<>();
    final Set<String> tempDirs = new HashSet<>();
    InputStream inputStream;
    try {
      inputStream = open.apply(schemasLocation);
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching from configured schemasLocation", e);
    }
    try (InputStream bi = new BufferedInputStream(inputStream);
        InputStream gzi = new GzipCompressorInputStream(bi);
        ArchiveInputStream i = new TarArchiveInputStream(gzi);) {
      ArchiveEntry entry;
      while ((entry = i.getNextEntry()) != null) {
        if (!i.canReadEntryData(entry)) {
          LOG.warn("Unable to read tar file entry: " + entry.getName());
          continue;
        }
        String[] components = entry.getName().split("/");
        if (components.length > 2 && "schemas".equals(components[1])) {
          String name = getAndCacheNormalizedPath(
              Arrays.copyOfRange(components, 2, components.length));
          if (entry.isDirectory()) {
            tempDirs.add(name);
            continue;
          }
          if (name.endsWith(schemaSuffix())) {
            tempSchemas.put(name, loadSchemaFromArchive(i));
          }
        }
      }
    }
    schemas = tempSchemas;
    dirs = tempDirs;
  }

  private void ensureSchemasLoaded() {
    if (schemas == null) {
      try {
        loadAllSchemas();
      } catch (IOException e) {
        throw new UncheckedIOException("Unexpected error while loading schemas", e);
      }
    }
  }

  /**
   * We normalize all path components to snake_case both when loading schemas and when fetching
   * schemas to deal with some naming inconsistencies in mozilla-pipeline-schemas.
   */
  private String getAndCacheNormalizedName(String name) {
    if (name == null) {
      return null;
    } else if (normalizedNameCache == null) {
      normalizedNameCache = CacheBuilder.newBuilder().maximumSize(50_000).build();
    }
    try {
      return normalizedNameCache.get(name, () -> SnakeCase.format(name));
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  private String getAndCacheNormalizedPath(String[] components) {
    return Arrays.stream(components).map(s -> {
      // We want to preserve the suffix after '.' unchanged, so we split before normalizing.
      String[] suffixSplit = s.split("\\.", 2);
      String suffix = suffixSplit.length > 1 ? "." + suffixSplit[1] : "";
      return getAndCacheNormalizedName(suffixSplit[0]) + suffix;
    }).collect(Collectors.joining("/"));
  }

  private String getAndCacheNormalizedPath(String path) {
    return getAndCacheNormalizedPath(path.split("/"));
  }

}
