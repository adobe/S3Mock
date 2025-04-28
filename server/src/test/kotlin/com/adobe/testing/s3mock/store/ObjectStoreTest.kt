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

import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.http.HttpHeaders
import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.UUID

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = [KmsKeyStore::class, BucketStore::class, MultipartStore::class])
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
  @Throws(Exception::class)
  fun testStoreObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val name = sourceFile.name
    val path = sourceFile.toPath()

    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME), id, name, null,
      storeHeaders(), path,
      emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD, ChecksumType.FULL_OBJECT
    ).also {
      assertThat(it.key).isEqualTo(name)
      assertThat(it.contentType).isEqualTo(DEFAULT_CONTENT_TYPE)
      assertThat(it.storeHeaders).containsEntry(HttpHeaders.CONTENT_ENCODING, ENCODING_GZIP)
      assertThat(it.etag).isEqualTo("\"${DigestUtil.hexDigest(Files.newInputStream(path))}\"")
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
    }

  }

  @Test
  @Throws(Exception::class)
  fun testStoreAndGetObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = sourceFile.name

    objectStore
      .storeS3ObjectMetadata(
        metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, storeHeaders(),
        path,
        emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.DEEP_ARCHIVE, ChecksumType.FULL_OBJECT
      )

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.key).isEqualTo(name)
      assertThat(it.contentType).isEqualTo(TEXT_PLAIN)
      assertThat(it.storeHeaders).containsEntry(HttpHeaders.CONTENT_ENCODING, ENCODING_GZIP)
      assertThat(it.etag).isEqualTo("\"${DigestUtil.hexDigest(Files.newInputStream(path))}\"")
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(it.storageClass).isEqualTo(StorageClass.DEEP_ARCHIVE)
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
    }

  }

  @Test
  @Throws(Exception::class)
  fun testStoreAndGetObject_startsWithSlash() {
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()
    val id = managedId()
    val name = "/app/config/" + sourceFile.name

    objectStore
      .storeS3ObjectMetadata(
        metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN, storeHeaders(),
        path,
        emptyMap(), emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD, ChecksumType.FULL_OBJECT
      )

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.key).isEqualTo(name)
      assertThat(it.contentType).isEqualTo(TEXT_PLAIN)
      assertThat(it.storeHeaders).containsEntry(HttpHeaders.CONTENT_ENCODING, ENCODING_GZIP)
      assertThat(it.etag).isEqualTo("\"${DigestUtil.hexDigest(Files.newInputStream(path))}\"")
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
    }

  }

  @Test
  fun testStoreAndGetObjectWithTags() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val name = sourceFile.name
    val tags = listOf(Tag("foo", "bar"))

    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
      storeHeaders(), sourceFile.toPath(),
      NO_USER_METADATA, emptyMap(), null, tags, null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD, ChecksumType.FULL_OBJECT
    )

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.tags[0].key).isEqualTo("foo")
      assertThat(it.tags[0].value).isEqualTo("bar")
    }
  }

  @Test
  fun testStoreAndGetTagsOnExistingObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val name = sourceFile.name

    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
      storeHeaders(), sourceFile.toPath(),
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD, ChecksumType.FULL_OBJECT
    )

    objectStore.storeObjectTags(metadataFrom(TEST_BUCKET_NAME), id, null, listOf(Tag("foo", "bar")))
    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.tags[0].key).isEqualTo("foo")
      assertThat(it.tags[0].value).isEqualTo("bar")
    }
  }

  @Test
  fun testStoreAndGetRetentionOnExistingObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val name = sourceFile.name

    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
      storeHeaders(), sourceFile.toPath(),
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD, ChecksumType.FULL_OBJECT
    )

    //TODO: resolution of time seems to matter here. Is this a serialization problem?
    val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    val retention = Retention(Mode.COMPLIANCE, now)
    objectStore.storeRetention(metadataFrom(TEST_BUCKET_NAME), id, null, retention)

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.retention).isNotNull()
      assertThat(it.retention.mode).isEqualTo(Mode.COMPLIANCE)
      assertThat(it.retention.retainUntilDate).isEqualTo(now)
    }

  }

  @Test
  fun testStoreAndGetLegalHoldOnExistingObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val name = sourceFile.name

    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME), id, name, TEXT_PLAIN,
      storeHeaders(), sourceFile.toPath(),
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD, ChecksumType.FULL_OBJECT
    )

    val legalHold = LegalHold(LegalHold.Status.ON)
    objectStore.storeLegalHold(metadataFrom(TEST_BUCKET_NAME), id, null, legalHold)
    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.legalHold).isNotNull()
      assertThat(it.legalHold.status).isEqualTo(LegalHold.Status.ON)
    }

  }

  @Test
  fun testStoreAndCopyObject() {
    val destinationObjectName = "destinationObject"
    val destinationBucketName = "destinationBucket"
    val sourceId = managedId()
    val destinationId = managedId()
    val sourceFile = File(TEST_FILE_PATH)

    val sourceBucketName = "sourceBucket"
    val sourceObjectName = sourceFile.name

    objectStore.storeS3ObjectMetadata(
      metadataFrom(sourceBucketName), sourceId, sourceObjectName,
      TEXT_PLAIN, storeHeaders(), sourceFile.toPath(),
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.GLACIER, ChecksumType.FULL_OBJECT
    )

    objectStore.copyS3Object(
      metadataFrom(sourceBucketName), sourceId, null,
      metadataFrom(destinationBucketName),
      destinationId, destinationObjectName, emptyMap(), emptyMap(), NO_USER_METADATA, StorageClass.STANDARD_IA
    )

    objectStore.getS3ObjectMetadata(metadataFrom(destinationBucketName), destinationId, null).also {
      assertThat(it.encryptionHeaders).isEmpty()
      assertThat(sourceFile).hasSameBinaryContentAs(it.dataPath.toFile())
      assertThat(it.storageClass).isEqualTo(StorageClass.STANDARD_IA)
    }

  }

  @Test
  @Throws(Exception::class)
  fun testStoreAndCopyObjectEncrypted() {
    val destinationObjectName = "destinationObject"
    val destinationBucketName = "destinationBucket"
    val sourceId = managedId()
    val destinationId = managedId()
    val sourceFile = File(TEST_FILE_PATH)
    val path = sourceFile.toPath()

    val sourceBucketName = "sourceBucket"
    val sourceObjectName = sourceFile.name

    objectStore.storeS3ObjectMetadata(
      metadataFrom(sourceBucketName), sourceId, sourceObjectName,
      TEXT_PLAIN, storeHeaders(), path,
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD, ChecksumType.FULL_OBJECT
    )

    objectStore.copyS3Object(
      metadataFrom(sourceBucketName),
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
      assertThat(it.encryptionHeaders).isEqualTo(encryptionHeaders())
      assertThat(it.size).isEqualTo(sourceFile.length().toString())
      assertThat(it.etag).isEqualTo("\"${DigestUtil.hexDigest(TEST_ENC_KEY, Files.newInputStream(path))}\"")
    }

  }

  @Test
  fun testStoreAndDeleteObject() {
    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val objectName = sourceFile.name

    objectStore
      .storeS3ObjectMetadata(
        metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
        storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD, ChecksumType.FULL_OBJECT
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
    val owner = Owner(
        "mtd@amazon.com",
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a"
    )
    val grantee = CanonicalUser(owner.displayName, owner.id)
    val policy = AccessControlPolicy(
      owner,
      listOf(Grant(grantee, Grant.Permission.FULL_CONTROL))
    )

    val sourceFile = File(TEST_FILE_PATH)
    val id = managedId()
    val objectName = sourceFile.name
    objectStore
      .storeS3ObjectMetadata(
        metadataFrom(TEST_BUCKET_NAME), id, objectName, TEXT_PLAIN,
        storeHeaders(), sourceFile.toPath(),
        NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
        StorageClass.STANDARD, ChecksumType.FULL_OBJECT
      )
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    objectStore.storeAcl(bucket, id, null, policy)

    val actual = objectStore.readAcl(bucket, id, null)

    assertThat(actual).isEqualTo(policy)
  }

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
    arrayListOf<UUID>().apply {
      for (id in idCache) {
        objectStore.deleteObject(metadataFrom(TEST_BUCKET_NAME), id, null)
        objectStore.deleteObject(metadataFrom("bucket1"), id, null)
        objectStore.deleteObject(metadataFrom("bucket2"), id, null)
        objectStore.deleteObject(metadataFrom("destinationBucket"), id, null)
        objectStore.deleteObject(metadataFrom("sourceBucket"), id, null)
        this.add(id)
      }
    }.also {
      for (id in it) {
        idCache.remove(id)
      }
    }

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
