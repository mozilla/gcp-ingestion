package com.mozilla.telemetry.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ArtifactRegistryInputStreamTest {

  private static final String RESOURCE_NAME = "projects/moz-fx-dev-whd-svcse-4252"
      + "/locations/us-west1/repositories/artifacts-generic"
      + "/files/schemas:ad36a04dc:schemas.tar.gz";

  @Test
  public void testHandlesArtifactRegistryResourceNames() {
    assertTrue(ArtifactRegistryInputStream.handles(RESOURCE_NAME));
  }

  @Test
  public void testDoesNotHandleOtherLocations() {
    assertFalse(ArtifactRegistryInputStream.handles("gs://my-bucket/schemas.tar.gz"));
    assertFalse(ArtifactRegistryInputStream.handles("/local/path/schemas.tar.gz"));
    assertFalse(ArtifactRegistryInputStream
        .handles("https://github.com/mozilla-services/mozilla-pipeline-schemas/archive/x.tar.gz"));
  }

  @Test
  public void testToDownloadUrlEncodesFileId() {
    assertEquals(
        "https://artifactregistry.googleapis.com/download/v1/projects/moz-fx-dev-whd-svcse-4252"
            + "/locations/us-west1/repositories/artifacts-generic"
            + "/files/schemas%3Aad36a04dc%3Aschemas.tar.gz:download?alt=media",
        ArtifactRegistryInputStream.toDownloadUrl(RESOURCE_NAME));
  }
}
