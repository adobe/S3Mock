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

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureWebMvc
import org.springframework.http.HttpHeaders
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.io.File
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.UUID
import kotlin.io.path.inputStream

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockitoBean(types = [KmsKeyStore::class, BucketStore::class, MultipartStore::class])
@SpringBootTest(classes = [StoreConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Execution(ExecutionMode.SAME_THREAD)
internal class ObjectStoreTest : StoreTestBase() {
  @Autowired
  private lateinit var objectStore: ObjectStore

  @BeforeEach
  fun beforeEach() {
    assertThat(idCache).isEmpty()
  }

  @Test
  fun testStoreObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val name = sourceFile.name
    val path = sourceFile.toPath()

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
      contentType = null,
    ).also {
      assertThat(it.key).isEqualTo(name)
      assertThat(it.contentType).isEqualTo(DEFAULT_CONTENT_TYPE)
      assertThat(it.storeHeaders).containsEntry(HttpHeaders.CONTENT_ENCODING, ENCODING_GZIP)
      assertThat(it.etag).isEqualTo(DigestUtil.hexDigest(path.inputStream()))
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
    }

  }

  @Test
  fun testStoreAndGetObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
      storageClass = StorageClass.DEEP_ARCHIVE,
    )

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it!!.key).isEqualTo(name)
      assertThat(it.contentType).isEqualTo(TEXT_PLAIN)
      assertThat(it.storeHeaders).containsEntry(HttpHeaders.CONTENT_ENCODING, ENCODING_GZIP)
      assertThat(it.etag).isEqualTo(DigestUtil.hexDigest(path.inputStream()))
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(it.storageClass).isEqualTo(StorageClass.DEEP_ARCHIVE)
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
    }

  }

  @Test
  fun testStoreAndGetObject_startsWithSlash() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = "/app/config/" + sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
    )

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it!!.key).isEqualTo(name)
      assertThat(it.contentType).isEqualTo(TEXT_PLAIN)
      assertThat(it.storeHeaders).containsEntry(HttpHeaders.CONTENT_ENCODING, ENCODING_GZIP)
      assertThat(it.etag).isEqualTo(DigestUtil.hexDigest(path.inputStream()))
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
    }

  }

  @Test
  fun testStoreAndGetObjectWithTags() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name
    val tags = listOf(Tag("foo", "bar"))

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
      tags = tags,
    )
    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it!!.tags?.get(0)?.key).isEqualTo("foo")
      assertThat(it.tags?.get(0)?.value).isEqualTo("bar")
    }
  }

  @Test
  fun testStoreAndGetTagsOnExistingObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
    )

    objectStore.storeTags(metadataFrom(TEST_BUCKET_NAME), id, null, listOf(Tag("foo", "bar")))
    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it!!.tags?.get(0)?.key).isEqualTo("foo")
      assertThat(it.tags?.get(0)?.value).isEqualTo("bar")
    }
  }

  @Test
  fun testStoreAndGetRetentionOnExistingObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
    )

    //TODO: resolution of time seems to matter here. Is this a serialization problem?
    val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    val retention = Retention(Mode.COMPLIANCE, now)
    objectStore.storeRetention(metadataFrom(TEST_BUCKET_NAME), id, null, retention)

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it!!.retention).isNotNull()
      assertThat(it.retention!!.mode).isEqualTo(Mode.COMPLIANCE)
      assertThat(it.retention.retainUntilDate).isEqualTo(now)
    }

  }

  @Test
  fun testStoreAndGetLegalHoldOnExistingObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
    )

    val legalHold = LegalHold(LegalHold.Status.ON)
    objectStore.storeLegalHold(metadataFrom(TEST_BUCKET_NAME), id, null, legalHold)
    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it!!.legalHold).isNotNull()
      assertThat(it.legalHold!!.status).isEqualTo(LegalHold.Status.ON)
    }

  }

  @Test
  fun testStoreAndCopyObject() {
    val destinationObjectName = "destinationObject"
    val destinationBucketName = "destinationBucket"
    val sourceId = managedId()
    val destinationId = managedId()
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()

    val sourceBucketName = "sourceBucket"
    val sourceObjectName = sourceFile.name
    val sourceBucketMetadata = metadataFrom(sourceBucketName)

    givenStoredS3ObjectMetadata(
      sourceId,
      sourceObjectName,
      path,
      bucketMetadata = sourceBucketMetadata,
      storageClass = StorageClass.GLACIER,
    )

    objectStore.copyObject(
      sourceBucketMetadata,
      sourceId,
      null,
      metadataFrom(destinationBucketName),
      destinationId,
      destinationObjectName,
      emptyMap(),
      emptyMap(),
      NO_USER_METADATA,
      StorageClass.STANDARD_IA
    )

    objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId, null).also {
      assertThat(it!!.encryptionHeaders).isEmpty()
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
      assertThat(it.storageClass).isEqualTo(StorageClass.STANDARD_IA)
    }

  }

  @Test
  fun testStoreAndCopyObjectEncrypted() {
    val destinationObjectName = "destinationObject"
    val destinationBucketName = "destinationBucket"
    val sourceId = managedId()
    val destinationId = managedId()
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()

    val sourceBucketName = "sourceBucket"
    val sourceObjectName = sourceFile.name
    val sourceBucketMetadata = metadataFrom(sourceBucketName)

    givenStoredS3ObjectMetadata(
      sourceId,
      sourceObjectName,
      path,
      bucketMetadata = sourceBucketMetadata,
    )

    objectStore.copyObject(
      sourceBucketMetadata,
      sourceId,
      null,
      metadataFrom(destinationBucketName),
      destinationId,
      destinationObjectName,
      encryptionHeaders(),
      emptyMap(),
      NO_USER_METADATA,
      StorageClass.STANDARD_IA
    )
    objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId, null).also {
      assertThat(it!!.encryptionHeaders).isEqualTo(encryptionHeaders())
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.etag).isEqualTo(DigestUtil.hexDigest(TEST_ENC_KEY, path.inputStream()))
    }
  }

  @Test
  fun testStoreAndDeleteObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
    )

    objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it).isTrue()
    }
    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it).isNull()
    }

  }

  @Test
  fun testStoreAndRetrieveAcl() {
    val owner = Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a")
    val grantee = CanonicalUser(null, owner.id)
    val policy = AccessControlPolicy(
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL)),
      owner
    )

    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
    )

    val bucket = metadataFrom(TEST_BUCKET_NAME)
    objectStore.storeAcl(bucket, id, null, policy)

    val actual = objectStore.readAcl(bucket, id, null)

    assertThat(actual).isEqualTo(policy)
  }

  @Test
  fun pretendToCopy_requires_a_change() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name
    val bucket = metadataFrom("bucket-pretend")

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
      bucketMetadata = bucket,
    )

    // All optional params unchanged -> should throw INVALID_COPY_REQUEST_SAME_KEY
    assertThatThrownBy {
      objectStore.pretendToCopyObject(
        bucket,
        id,
        null,
        null,
        null,
        null,
        null
      )
    }.isSameAs(S3Exception.INVALID_COPY_REQUEST_SAME_KEY)
  }

  @Test
  fun pretendToCopy_with_changes_updates_metadata_and_persists() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name
    val bucket = metadataFrom("bucket-pretend")

    givenStoredS3ObjectMetadata(
      id,
      name,
      path,
      bucketMetadata = bucket,
    )

    val newUserMetadata = mapOf("x-amz-meta-key" to "value")
    val newStoreHeaders = mapOf("cache-control" to "no-cache")
    val newEncHeaders = encryptionHeaders()
    val newStorageClass = StorageClass.STANDARD_IA

    val updated = objectStore.pretendToCopyObject(
      bucket,
      id,
      null,
      newEncHeaders,
      newStoreHeaders,
      newUserMetadata,
      newStorageClass
    )!!

    // Verify fields changed accordingly
    assertThat(updated.userMetadata).isEqualTo(newUserMetadata)
    assertThat(updated.storeHeaders).isEqualTo(newStoreHeaders)
    assertThat(updated.encryptionHeaders).isEqualTo(newEncHeaders)
    assertThat(updated.storageClass).isEqualTo(newStorageClass)

    // And persisted to disk (re-read)
    val reread = objectStore.getS3ObjectMetadata(bucket, id, null)!!
    assertThat(reread.userMetadata).isEqualTo(newUserMetadata)
    assertThat(reread.storeHeaders).isEqualTo(newStoreHeaders)
    assertThat(reread.encryptionHeaders).isEqualTo(newEncHeaders)
    assertThat(reread.storageClass).isEqualTo(newStorageClass)
  }

  @Test
  fun store_with_checksum_fields_persists_checksum() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name
    val bucket = metadataFrom(TEST_BUCKET_NAME)

    val algo = ChecksumAlgorithm.SHA256

    val metadata = givenStoredS3ObjectMetadata(
      id,
      name,
      path,
      bucketMetadata = bucket,
      checksumAlgorithm = algo,
      checksumValue = "dummy-checksum", // We don't validate here; just persist
    )

    assertThat(metadata.checksumAlgorithm).isEqualTo(algo)
    assertThat(metadata.checksum).isEqualTo("dummy-checksum")

    val reread = objectStore.getS3ObjectMetadata(bucket, id, null)!!
    assertThat(reread.checksumAlgorithm).isEqualTo(algo)
    assertThat(reread.checksum).isEqualTo("dummy-checksum")
  }

  private fun givenStoredS3ObjectMetadata(
    id: UUID,
    name: String,
    path: Path,
    bucketMetadata: BucketMetadata = metadataFrom(TEST_BUCKET_NAME),
    contentType: String? = TEXT_PLAIN,
    storeHeaders: Map<String, String> = storeHeaders(),
    userMetadata: Map<String, String> = NO_USER_METADATA,
    encryptionHeaders: Map<String, String> = emptyMap(),
    etag: String? = null,
    tags: List<Tag>? = emptyList(),
    checksumAlgorithm: ChecksumAlgorithm? = null,
    checksumValue: String? = null,
    owner: Owner = Owner.DEFAULT_OWNER,
    storageClass: StorageClass = StorageClass.STANDARD,
    checksumType: ChecksumType = ChecksumType.FULL_OBJECT
  ): S3ObjectMetadata = objectStore.storeS3ObjectMetadata(
    bucketMetadata,
    id,
    name,
    contentType,
    storeHeaders,
    path,
    userMetadata,
    encryptionHeaders,
    etag,
    tags,
    checksumAlgorithm,
    checksumValue,
    owner,
    storageClass,
    checksumType
  )

  private fun managedId(): UUID {
    val uuid = UUID.randomUUID()
    idCache.add(uuid)
    return uuid
  }

  /**
   * Deletes all created files from disk.
   */
  @AfterEach
  fun cleanupStores() {
    val ids = idCache.toList()
    val buckets = listOf(TEST_BUCKET_NAME, "bucket1", "bucket2", "destinationBucket", "sourceBucket")
    ids.forEach { id ->
      buckets.forEach { bucket ->
        objectStore.deleteObject(metadataFrom(bucket), id, null)
      }
    }
    idCache.removeAll(ids.toSet())
  }

  companion object {
    private val idCache: MutableList<UUID> = Collections.synchronizedList(ArrayList())

    @JvmStatic
    @AfterAll
    fun afterAll() {
      assertThat(idCache).isEmpty()
    }
  }
}
