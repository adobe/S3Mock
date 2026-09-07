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
package com.adobe.testing.s3mock.s3.store

import com.adobe.testing.s3mock.s3.dto.ObjectOwnership
import com.adobe.testing.s3mock.s3.model.BucketMetadata
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import kotlin.io.path.listDirectoryEntries

internal class StoreConfigurationTest {
  @Test
  @Throws(IOException::class)
  fun bucketCreation_noExistingBuckets(
    @TempDir tempDir: Path,
  ) {
    val initialBucketName = "initialBucketName"

    val properties = StoreProperties(false, "", setOf(), listOf(initialBucketName), "eu-central-1")
    val iut = StoreConfiguration()
    val bucketStore =
      iut.bucketStore(
        properties,
        tempDir.toFile(),
        listOf(),
        OBJECT_MAPPER,
        "eu-central-1",
      )

    assertThat(bucketStore.getBucketMetadata(initialBucketName).name).isEqualTo(initialBucketName)

    val createdBuckets = tempDir.listDirectoryEntries()
    assertThat(createdBuckets).hasSize(1)
    assertThat(createdBuckets[0].fileName).hasToString(initialBucketName)
    assertThat(bucketStore.getBucketMetadata(initialBucketName).path).isEqualTo(createdBuckets[0])
  }

  @Test
  @Throws(IOException::class)
  fun bucketCreation_existingBuckets(
    @TempDir tempDir: Path,
  ) {
    val existingBucketName = "existingBucketName"
    val existingBucket = Paths.get(tempDir.toAbsolutePath().toString(), existingBucketName)
    existingBucket.toFile().mkdirs()
    val bucketMetadata =
      BucketMetadata(
        existingBucketName,
        Instant.now().toString(),
        null,
        null,
        null,
        ObjectOwnership.BUCKET_OWNER_ENFORCED,
        existingBucket,
        "eu-central-1",
        null,
        null,
      )
    val metaFile = Paths.get(existingBucket.toString(), BUCKET_META_FILE)
    OBJECT_MAPPER.writeValue(metaFile.toFile(), bucketMetadata)

    val initialBucketName = "initialBucketName"

    val properties = StoreProperties(false, "", setOf(), listOf(initialBucketName), "eu-central-1")
    val iut = StoreConfiguration()
    val bucketStore =
      iut.bucketStore(
        properties,
        tempDir.toFile(),
        listOf(existingBucketName),
        OBJECT_MAPPER,
        "eu-central-1",
      )

    assertThat(bucketStore.getBucketMetadata(initialBucketName).name)
      .isEqualTo(initialBucketName)

    val createdBuckets = tempDir.listDirectoryEntries()
    assertThat(createdBuckets)
      .hasSize(2)
      .containsExactlyInAnyOrder(
        bucketStore.getBucketMetadata(existingBucketName).path,
        bucketStore.getBucketMetadata(initialBucketName).path,
      ).extracting<Path, RuntimeException> { obj: Path -> obj.fileName }
      .containsExactlyInAnyOrder(Path.of(existingBucketName), Path.of(initialBucketName))
  }

  @Test
  fun bucketCreation_ignoresBlankInitialBuckets(
    @TempDir tempDir: Path,
  ) {
    val properties = StoreProperties(false, "", setOf(), listOf(""), "eu-central-1")
    val iut = StoreConfiguration()
    val bucketStore =
      iut.bucketStore(
        properties,
        tempDir.toFile(),
        listOf(),
        OBJECT_MAPPER,
        "eu-central-1",
      )

    assertThat(bucketStore.listBuckets()).isEmpty()
    assertThat(tempDir.listDirectoryEntries()).isEmpty()
  }

  @Test
  fun rootFolder_usesExistingWritableDirectory(
    @TempDir tempDir: Path,
  ) {
    val existing = tempDir.resolve("existingRoot")
    assertThat(existing.toFile().mkdir()).isTrue()

    val properties = StoreProperties(false, existing.toAbsolutePath().toString(), setOf(), listOf(), "us-east-1")
    val root = StoreConfiguration().rootFolder(properties)

    assertThat(root).isEqualTo(existing.toFile())
    assertThat(root.canWrite()).isTrue()
  }

  @Test
  fun rootFolder_createsMissingDirectory(
    @TempDir tempDir: Path,
  ) {
    val missing = tempDir.resolve("createdRoot")

    val properties = StoreProperties(false, missing.toAbsolutePath().toString(), setOf(), listOf(), "us-east-1")
    val root = StoreConfiguration().rootFolder(properties)

    assertThat(root).isEqualTo(missing.toFile())
    assertThat(root).exists()
  }

  @Test
  fun rootFolder_failsFastWhenRootIsNotWritable(
    @TempDir tempDir: Path,
  ) {
    val readOnly = tempDir.resolve("readOnlyRoot")
    assertThat(readOnly.toFile().mkdir()).isTrue()
    assertThat(readOnly.toFile().setWritable(false, false)).isTrue()
    assumeFalse(readOnly.toFile().canWrite(), "Unable to make temp dir read-only (running as root?)")
    try {
      val properties = StoreProperties(false, readOnly.toAbsolutePath().toString(), setOf(), listOf(), "us-east-1")

      assertThatThrownBy { StoreConfiguration().rootFolder(properties) }
        .isInstanceOf(IllegalStateException::class.java)
        .hasMessageContaining(readOnly.toFile().absolutePath)
        .hasMessageContaining("not writable")
        .hasMessageContaining("named Docker volume")
    } finally {
      readOnly.toFile().setWritable(true, false)
    }
  }

  companion object {
    private const val BUCKET_META_FILE = "bucketMetadata.json"
    private val OBJECT_MAPPER: ObjectMapper =
      JsonMapper
        .builder()
        // Ensure Kotlin/JavaTime/etc. modules are discovered similarly to Boot
        .addModule(KotlinModule.Builder().build())
        .findAndAddModules()
        // Align with Boot defaults
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build()
  }
}
