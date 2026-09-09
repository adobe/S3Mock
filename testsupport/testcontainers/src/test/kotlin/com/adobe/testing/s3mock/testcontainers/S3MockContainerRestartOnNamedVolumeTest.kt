/*
 *  Copyright 2017-2026 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.testing.s3mock.testcontainers

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.testcontainers.DockerClientFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import java.net.URI
import java.util.UUID

/**
 * Verifies that data written by a non-root S3Mock container survives a full container restart when
 * the store root is backed by a Docker named volume mounted at `/s3mockroot`.
 *
 * This is the regression test for https://github.com/adobe/S3Mock/issues/3139: after the migration
 * to the Cloud Native Buildpacks image (which runs as the non-root `cnb` user), a named volume was
 * no longer writable. The OCI image now pre-creates a `cnb`-owned `/s3mockroot`, so Docker gives a
 * named volume mounted there the same ownership and the non-root process can persist to it — without
 * running the container as root (contrast [S3MockContainerRestartOnRootfolderTest], which uses a
 * bind mount and must run as root).
 */
internal class S3MockContainerRestartOnNamedVolumeTest {
  @Test
  fun objectSurvivesContainerRestartOnNamedVolume() {
    val bucketName = "named-volume-bucket"
    val key = "persisted.txt"
    val content = "persisted across a container restart"

    // First container: write an object to the named-volume-backed store root as the non-root user.
    newContainer().use { container ->
      val s3Client = createS3ClientV2(container.httpEndpoint)
      s3Client.createBucket { it.bucket(bucketName) }
      s3Client.putObject(
        { it.bucket(bucketName).key(key) },
        RequestBody.fromString(content),
      )
    }

    // Second container: mount the same named volume and assert the object is still there.
    newContainer().use { container ->
      val s3Client = createS3ClientV2(container.httpEndpoint)
      val response = s3Client.getObjectAsBytes { it.bucket(bucketName).key(key) }
      assertThat(response.asUtf8String()).isEqualTo(content)
    }
  }

  private fun newContainer(): S3MockContainer =
    S3MockContainer(S3MOCK_VERSION)
      .withRetainFilesOnExit(true)
      .withNamedVolume(VOLUME_NAME)
      .apply { start() }

  private fun createS3ClientV2(endpoint: String): S3Client =
    S3Client
      .builder()
      .region(Region.of("us-east-1"))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")),
      ).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .endpointOverride(URI.create(endpoint))
      .build()

  companion object {
    // we set the system property when running in maven, use "latest" for unit tests in the IDE
    private val S3MOCK_VERSION: String = System.getProperty("s3mock.version", "latest")
    private val VOLUME_NAME: String = "s3mock-named-volume-test-${UUID.randomUUID()}"

    @JvmStatic
    @AfterAll
    fun removeNamedVolume() {
      runCatching {
        DockerClientFactory
          .instance()
          .client()
          .removeVolumeCmd(VOLUME_NAME)
          .exec()
      }
    }
  }
}
