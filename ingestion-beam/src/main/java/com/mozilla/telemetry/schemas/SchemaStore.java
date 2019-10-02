package com.mozilla.telemetry.schemas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mozilla.telemetry.util.SnakeCase;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.text.StringSubstitutor;

/** Reads schemas from a file and parses them into a map. */
public abstract class SchemaStore<T> implements Serializable {

  /** Return the parsed schema corresponding to a path underneath the schemas/ directory. */
  public T getSchema(String path) throws SchemaNotFoundException {
    ensureSchemasLoaded();
    T schema = schemas.get(getAndCacheNormalizedPath(path));
    if (schema == null) {
      throw SchemaNotFoundException.forName(path);
    }
    return schema;
  }

  /** Return the parsed schema corresponding to doctype and namespace parsed from attributes. */
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

  protected SchemaStore(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    this.schemasLocation = schemasLocation;
    this.schemaAliasesLocation = schemaAliasesLocation;
  }

  /* Returns the expected suffix for a particular schema type e.g. `.schema.json` */
  protected abstract String schemaSuffix();

  /* Returns a parsed schema from an archive input stream. */
  protected abstract T loadSchemaFromArchive(ArchiveInputStream archive) throws IOException;

  ////////

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SchemaStore.class);

  private final ValueProvider<String> schemasLocation;
  @Nullable
  private final ValueProvider<String> schemaAliasesLocation;
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
      Metadata metadata = FileSystems.matchSingleFileSpec(schemasLocation.get());
      ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
      inputStream = Channels.newInputStream(channel);
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching from configured schemasLocation", e);
    }
    try (InputStream bi = new BufferedInputStream(inputStream);
        InputStream gzi = new GzipCompressorInputStream(bi);
        ArchiveInputStream i = new TarArchiveInputStream(gzi);) {
      ArchiveEntry entry = null;
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

  private void loadSchemaAliases() throws IOException {
    final Set<String> tempDirs = new HashSet<>();
    final Map<String, T> tempSchemas = new HashMap<>();
    Multimap<String, String> schemaAliasesMap;
    try {
      Multimap<String, String> aliases = ArrayListMultimap.create();
      if (schemaAliasesLocation != null && schemaAliasesLocation.isAccessible()
          && !Strings.isNullOrEmpty(schemaAliasesLocation.get())) {
        Metadata metadata = FileSystems.matchSingleFileSpec(schemaAliasesLocation.get());
        InputStream configInputStream = Channels
            .newInputStream(FileSystems.open(metadata.resourceId()));

        ObjectMapper objectMapper = new ObjectMapper();
        SchemaAliasingConfiguration aliasingConfig = objectMapper.readValue(configInputStream,
            SchemaAliasingConfiguration.class);

        aliasingConfig.aliases().forEach((e) -> {
          aliases.put(getAndCacheNormalizedPath(e.base()), getAndCacheNormalizedPath(e.alias()));
        });
      }
      schemaAliasesMap = aliases;
    } catch (IOException e) {
      throw new IOException("Exception thrown while fetching from configured schemaAliasesLocation",
          e);
    }

    dirs.forEach(dir -> {
      tempDirs.add(dir);
      tempDirs.addAll(schemaAliasesMap.get(dir));
    });

    schemas.forEach((schemaName, schema) -> {
      tempSchemas.put(schemaName, schema);

      String[] components = schemaName.split("/");
      String namespaceWithDocType = components[0] + "/" + components[1];
      // if aliases are defined for current `schemaName`, assemble new aliasing path and put it to
      // the map with original schema
      if (schemaAliasesMap.containsKey(namespaceWithDocType)) {
        for (String namespaceWithDocTypeAlias : schemaAliasesMap.get(namespaceWithDocType)) {
          String docTypeWithVersion = components[components.length - 1];
          String versionAndSuffix = docTypeWithVersion.substring(docTypeWithVersion.indexOf("."));

          String docTypeAlias = namespaceWithDocTypeAlias.split("/")[1];
          String docTypeWithVersionAlias = docTypeAlias + versionAndSuffix;

          tempSchemas.put(namespaceWithDocTypeAlias + "/" + docTypeWithVersionAlias, schema);
        }
      }
    });

    schemas = tempSchemas;
    dirs = tempDirs;
  }

  private void ensureSchemasLoaded() {
    if (schemas == null) {
      try {
        loadAllSchemas();
        loadSchemaAliases();
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
    } catch (ExecutionException e) {
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
