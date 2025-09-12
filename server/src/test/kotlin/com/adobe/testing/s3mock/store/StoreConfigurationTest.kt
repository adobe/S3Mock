/*
 *  Copyright 2017-2025 Adobe.
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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import software.amazon.awssdk.regions.Region
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

internal class StoreConfigurationTest {
  @Test
  @Throws(IOException::class)
  fun bucketCreation_noExistingBuckets(@TempDir tempDir: Path) {
    val initialBucketName = "initialBucketName"

    val properties = StoreProperties(false, "", setOf(), listOf(initialBucketName), Region.EU_CENTRAL_1)
    val iut = StoreConfiguration()
    val bucketStore = iut.bucketStore(
      properties,
      tempDir.toFile(),
      listOf(),
      OBJECT_MAPPER,
      Region.EU_CENTRAL_1
    )

    assertThat(bucketStore.getBucketMetadata(initialBucketName).name).isEqualTo(initialBucketName)

    val createdBuckets = Files.newDirectoryStream(tempDir).use { ds ->
      ds.toList()
    }
    assertThat(createdBuckets).hasSize(1)
    assertThat(createdBuckets[0].fileName).hasToString(initialBucketName)
    assertThat(bucketStore.getBucketMetadata(initialBucketName).path).isEqualTo(createdBuckets[0])
  }


  @Test
  @Throws(IOException::class)
  fun bucketCreation_existingBuckets(@TempDir tempDir: Path) {
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

    val properties = StoreProperties(false, "", setOf(), listOf(initialBucketName), Region.EU_CENTRAL_1)
    val iut = StoreConfiguration()
    val bucketStore =
      iut.bucketStore(
        properties,
        tempDir.toFile(),
        listOf(existingBucketName),
        OBJECT_MAPPER,
        Region.EU_CENTRAL_1
      )

    assertThat(bucketStore.getBucketMetadata(initialBucketName).name)
      .isEqualTo(initialBucketName)

    val createdBuckets = Files.newDirectoryStream(tempDir).use { ds ->
      ds.toList()
    }
    assertThat(createdBuckets)
      .hasSize(2)
      .containsExactlyInAnyOrder(
        bucketStore.getBucketMetadata(existingBucketName).path,
        bucketStore.getBucketMetadata(initialBucketName).path
      )
      .extracting<Path, RuntimeException> { obj: Path -> obj.fileName }
      .containsExactlyInAnyOrder(Path.of(existingBucketName), Path.of(initialBucketName))
  }

  companion object {
    private const val BUCKET_META_FILE = "bucketMetadata.json"
    private val OBJECT_MAPPER = ObjectMapper()
  }
}
