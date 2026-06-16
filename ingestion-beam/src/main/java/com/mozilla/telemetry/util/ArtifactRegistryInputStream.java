package com.mozilla.telemetry.util;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Opens a file in an Artifact Registry generic repository given its resource name
 * ({@code projects/.../repositories/REPOSITORY/files/FILE}), authenticating with Application
 * Default Credentials.
 *
 * @see <a href="https://docs.cloud.google.com/artifact-registry/docs/generic#api_1">Artifact
 *     Registry generic repository API</a>
 */
public class ArtifactRegistryInputStream {

  private static final String DOWNLOAD_PREFIX = "https://artifactregistry.googleapis.com/download/v1/";
  private static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";
  private static final String FILES_SEGMENT = "/files/";

  /**
   * Returns true if {@code path} is an Artifact Registry file resource name.
   */
  public static boolean handles(String path) {
    return path.startsWith("projects/") && path.contains("/repositories/")
        && path.contains(FILES_SEGMENT);
  }

  /**
   * Open the file named by Artifact Registry resource name {@code location}.
   */
  public static InputStream open(String location) throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    if (credentials.createScopedRequired()) {
      credentials = credentials.createScoped(SCOPE);
    }
    HttpRequestFactory requestFactory = new NetHttpTransport()
        .createRequestFactory(new HttpCredentialsAdapter(credentials));
    HttpResponse response = requestFactory.buildGetRequest(new GenericUrl(toDownloadUrl(location)))
        .execute();
    return response.getContent();
  }

  /**
   * Convert a file resource name to its media download URL, url-encoding the file id segment.
   */
  static String toDownloadUrl(String resourceName) {
    int idStart = resourceName.indexOf(FILES_SEGMENT) + FILES_SEGMENT.length();
    String fileId = resourceName.substring(idStart);
    String encodedFileId = URLEncoder.encode(fileId, StandardCharsets.UTF_8).replace("+", "%20");
    return DOWNLOAD_PREFIX + resourceName.substring(0, idStart) + encodedFileId
        + ":download?alt=media";
  }
}
