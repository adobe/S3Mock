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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.StorageClass
import org.apache.commons.codec.digest.DigestUtils
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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
import org.springframework.http.HttpRange
import org.springframework.http.MediaType
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Collections
import java.util.Date
import java.util.UUID

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBean(classes = [KmsKeyStore::class, BucketStore::class])
@SpringBootTest(classes = [StoreConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Execution(ExecutionMode.SAME_THREAD)
internal class MultipartStoreTest : StoreTestBase() {
  @Autowired
  private lateinit var multipartStore: MultipartStore

  @Autowired
  private lateinit var objectStore: ObjectStore

  @Autowired
  private lateinit var rootFolder: File

  @BeforeEach
  fun beforeEach() {
    assertThat(idCache).isEmpty()
  }

  @Test
  fun shouldCreateMultipartUploadFolder() {
    val fileName = "aFile"
    val id = managedId()
    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    Paths.get(
      rootFolder.absolutePath,
      TEST_BUCKET_NAME,
      MultipartStore.MULTIPARTS_FOLDER,
      uploadId
    )
      .toFile().also {
        assertThat(it)
          .exists()
          .isDirectory()
      }

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId)
  }

  @Test
  fun shouldCreateMultipartUploadFolderIfBucketExists() {
    val fileName = "aFile"
    val id = managedId()
    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    Paths.get(
      rootFolder.absolutePath,
      TEST_BUCKET_NAME,
      MultipartStore.MULTIPARTS_FOLDER,
      uploadId
    )
      .toFile().also {
        assertThat(it)
          .exists()
          .isDirectory()
      }


    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId)
  }

  @Test
  @Throws(IOException::class)
  fun shouldStorePart() {
    val fileName = "PartFile"
    val partNumber = "1"
    val id = managedId()
    val part = "Part1"
    val tempFile = Files.createTempFile("", "")
    ByteArrayInputStream(part.toByteArray()).transferTo(Files.newOutputStream(tempFile))
    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    multipartStore.putPart(
      metadataFrom(TEST_BUCKET_NAME), id, uploadId, partNumber,
      tempFile, emptyMap()
    )
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME,
        MultipartStore.MULTIPARTS_FOLDER,
        uploadId,
        "$partNumber.part"
      ).toFile()
    ).exists()

    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId)
  }

  @Test
  @Throws(IOException::class)
  fun shouldFinishUpload() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    ByteArrayInputStream(part1.toByteArray()).transferTo(Files.newOutputStream(tempFile1))
    val tempFile2 = Files.createTempFile("", "")
    ByteArrayInputStream(part2.toByteArray()).transferTo(Files.newOutputStream(tempFile2))
    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    val multipartUploadInfo = multipartUploadInfo(multipartUpload)
    multipartStore
      .putPart(
        metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
        tempFile1, emptyMap()
      )
    multipartStore
      .putPart(
        metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2",
        tempFile2, emptyMap()
      )

    val result =
      multipartStore.completeMultipartUpload(
        metadataFrom(TEST_BUCKET_NAME), fileName, id,
        uploadId, getParts(2), emptyMap(), multipartUploadInfo, "location"
      )
    val allMd5s = DigestUtils.md5("Part1") + DigestUtils.md5("Part2")

    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME, id.toString(), "binaryData"
      ).toFile()
    ).exists()
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME, id.toString(), "objectMetadata.json"
      ).toFile()
    ).exists()
    assertThat(result.etag).isEqualTo("\"${DigestUtils.md5Hex(allMd5s)}-2\"")
  }

  @Test
  @Throws(IOException::class)
  fun hasValidMetadata() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    ByteArrayInputStream(part1.toByteArray()).transferTo(Files.newOutputStream(tempFile1))
    val tempFile2 = Files.createTempFile("", "")
    ByteArrayInputStream(part2.toByteArray()).transferTo(Files.newOutputStream(tempFile2))

    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    val multipartUploadInfo = multipartUploadInfo(multipartUpload)
    multipartStore
      .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1", tempFile1, emptyMap())
    multipartStore
      .putPart(metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2", tempFile2, emptyMap())

    multipartStore.completeMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id, uploadId,
      getParts(2), emptyMap(), multipartUploadInfo, "location"
    )

    objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null).also {
      assertThat(it.size).isEqualTo("10")
      assertThat(it.contentType).isEqualTo(MediaType.APPLICATION_OCTET_STREAM.toString())
    }
  }

  private fun getParts(n: Int): List<CompletedPart> {
    val parts = ArrayList<CompletedPart>()
    for (i in 1..n) {
      parts.add(CompletedPart(i, null, null, null, null, null))
    }
    return parts
  }

  @Test
  @Throws(IOException::class)
  fun returnsValidPartsFromMultipart() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    ByteArrayInputStream(part1.toByteArray()).transferTo(Files.newOutputStream(tempFile1))
    val tempFile2 = Files.createTempFile("", "")
    ByteArrayInputStream(part2.toByteArray()).transferTo(Files.newOutputStream(tempFile2))

    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId

    multipartStore.putPart(
      metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1", tempFile1,
      emptyMap()
    )
    multipartStore.putPart(
      metadataFrom(TEST_BUCKET_NAME), id, uploadId, "2", tempFile2,
      emptyMap()
    )

    multipartStore.getMultipartUploadParts(metadataFrom(TEST_BUCKET_NAME), id, uploadId).also {
      assertThat(it).hasSize(2)

      val expectedPart1 = prepareExpectedPart(1, it[0].lastModified, part1)
      val expectedPart2 = prepareExpectedPart(2, it[1].lastModified, part2)

      assertThat(it[0]).isEqualTo(expectedPart1)
      assertThat(it[1]).isEqualTo(expectedPart2)
    }


    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), id, uploadId)
  }

  private fun prepareExpectedPart(partNumber: Int, lastModified: Date, content: String): Part {
    return Part(
      partNumber,
      DigestUtils.md5Hex(content),
      lastModified,
      content.toByteArray().size.toLong()
    )
  }

  @Test
  @Throws(IOException::class)
  fun deletesTemporaryMultipartUploadFolder() {
    val fileName = "PartFile"
    val id = managedId()
    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    val multipartUploadInfo = multipartUploadInfo(multipartUpload)
    val tempFile = Files.createTempFile("", "")
    ByteArrayInputStream("Part1".toByteArray()).transferTo(Files.newOutputStream(tempFile))
    multipartStore
      .putPart(
        metadataFrom(TEST_BUCKET_NAME), id, uploadId, "1",
        tempFile, emptyMap()
      )

    multipartStore.completeMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), fileName, id, uploadId,
      getParts(1), emptyMap(), multipartUploadInfo, "location"
    )

    assertThat(
      Paths.get(rootFolder.absolutePath, TEST_BUCKET_NAME, fileName, uploadId)
        .toFile()
    ).doesNotExist()
  }

  @Test
  fun listsMultipartUploads() {
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty()

    val fileName = "PartFile"
    val id = managedId()
    val multipartUpload = multipartStore
      .createMultipartUpload(
        bucketMetadata, fileName, id, DEFAULT_CONTENT_TYPE, storeHeaders(),
        TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
        StorageClass.STANDARD, null, null
      )
    val uploadId = multipartUpload.uploadId
    val multipartUploadInfo = multipartUploadInfo(multipartUpload)
    val uploads = multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)
    assertThat(uploads).hasSize(1)
    uploads.iterator().next().also {
      assertThat(it).isEqualTo(multipartUpload)
      // and some specific sanity checks
      assertThat(it.uploadId).isEqualTo(uploadId)
      assertThat(it.key).isEqualTo(fileName)
    }

    multipartStore.completeMultipartUpload(
      bucketMetadata, fileName, id, uploadId, getParts(0),
      emptyMap(), multipartUploadInfo, "location"
    )

    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty()
  }

  @Test
  fun listsMultipartUploadsMultipleBuckets() {
    val bucketName1 = "bucket1"
    val bucketMetadata1 = metadataFrom(bucketName1)
    assertThat(multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX)).isEmpty()
    val bucketName2 = "bucket2"
    val bucketMetadata2 = metadataFrom(bucketName2)
    assertThat(multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX)).isEmpty()

    val fileName1 = "PartFile1"
    val id1 = managedId()
    val multipartUpload1 = multipartStore
      .createMultipartUpload(
        bucketMetadata1, fileName1, id1, DEFAULT_CONTENT_TYPE,
        storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
        StorageClass.STANDARD, null, null
      )
    val uploadId1 = multipartUpload1.uploadId
    val multipartUploadInfo1 = multipartUploadInfo(multipartUpload1)
    val fileName2 = "PartFile2"
    val id2 = managedId()
    val multipartUpload2 = multipartStore
      .createMultipartUpload(
        bucketMetadata2, fileName2, id2, DEFAULT_CONTENT_TYPE,
        storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
        StorageClass.STANDARD, null, null
      )
    val uploadId2 = multipartUpload2.uploadId
    val multipartUploadInfo2 = multipartUploadInfo(multipartUpload2)
    multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX).also {
      assertThat(it).hasSize(1)
      it[0].also {
        assertThat(it).isEqualTo(multipartUpload1)
        // and some specific sanity checks
        assertThat(it.uploadId).isEqualTo(uploadId1)
        assertThat(it.key).isEqualTo(fileName1)
      }
    }

    multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX).also {
      assertThat(it).hasSize(1)
      it[0].also {
        assertThat(it).isEqualTo(multipartUpload2)
        // and some specific sanity checks
        assertThat(it.uploadId).isEqualTo(uploadId2)
        assertThat(it.key).isEqualTo(fileName2)
      }
    }

    multipartStore.completeMultipartUpload(
      bucketMetadata1, fileName1, id1, uploadId1,
      getParts(0), emptyMap(), multipartUploadInfo1, "location"
    )
    multipartStore.completeMultipartUpload(
      bucketMetadata2, fileName2, id2, uploadId2,
      getParts(0), emptyMap(), multipartUploadInfo2, "location"
    )

    assertThat(multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX)).isEmpty()
    assertThat(multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX)).isEmpty()
  }

  @Test
  @Throws(IOException::class)
  fun abortMultipartUpload() {
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty()

    val fileName = "PartFile"
    val id = managedId()
    val multipartUpload = multipartStore.createMultipartUpload(
      bucketMetadata, fileName, id,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    val tempFile = Files.createTempFile("", "")
    ByteArrayInputStream("Part1".toByteArray()).transferTo(Files.newOutputStream(tempFile))
    multipartStore.putPart(
      bucketMetadata, id, uploadId, "1",
      tempFile, emptyMap()
    )
    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).hasSize(1)

    multipartStore.abortMultipartUpload(bucketMetadata, id, uploadId)

    assertThat(multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX)).isEmpty()
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME, fileName, "binaryData"
      ).toFile()
    ).doesNotExist()
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME, fileName, "objectMetadata.json"
      ).toFile()
    ).doesNotExist()
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME, fileName, uploadId
      ).toFile()
    ).doesNotExist()
  }

  @Test
  @Throws(IOException::class)
  fun copyPart() {
    val sourceFile = UUID.randomUUID().toString()
    val sourceId = managedId()
    val targetFile = UUID.randomUUID().toString()
    val partNumber = "1"
    val destinationId = managedId()

    val contentBytes = UUID.randomUUID().toString().toByteArray()
    val tempFile = Files.createTempFile("", "")
    ByteArrayInputStream(contentBytes).transferTo(Files.newOutputStream(tempFile))
    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME), sourceId, sourceFile,
      DEFAULT_CONTENT_TYPE, storeHeaders(), tempFile,
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD
    )

    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), targetFile, destinationId,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId

    val range = HttpRange.createByteRange(0, contentBytes.size.toLong())
    multipartStore.copyPart(
      metadataFrom(TEST_BUCKET_NAME), sourceId, range, partNumber,
      metadataFrom(TEST_BUCKET_NAME), destinationId, uploadId, emptyMap(), null
    )
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME,
        MultipartStore.MULTIPARTS_FOLDER,
        uploadId, "$partNumber.part"
      )
        .toFile()
    ).exists()
    multipartStore.abortMultipartUpload(metadataFrom(TEST_BUCKET_NAME), destinationId, uploadId)
  }

  @Test
  @Throws(IOException::class)
  fun copyPartNoRange() {
    val sourceFile = UUID.randomUUID().toString()
    val sourceId = managedId()
    val targetFile = UUID.randomUUID().toString()
    val partNumber = "1"
    val destinationId = managedId()
    val contentBytes = UUID.randomUUID().toString().toByteArray()
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    val tempFile = Files.createTempFile("", "")
    ByteArrayInputStream(contentBytes)
      .transferTo(Files.newOutputStream(tempFile))
    objectStore.storeS3ObjectMetadata(
      bucketMetadata, sourceId, sourceFile, DEFAULT_CONTENT_TYPE,
      storeHeaders(), tempFile,
      NO_USER_METADATA, emptyMap(), null, emptyList(), null, null, Owner.DEFAULT_OWNER,
      StorageClass.STANDARD
    )

    val multipartUpload = multipartStore.createMultipartUpload(
      bucketMetadata, targetFile, destinationId,
      DEFAULT_CONTENT_TYPE, storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA,
      emptyMap(), StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId

    multipartStore.copyPart(
      bucketMetadata, sourceId, null, partNumber,
      bucketMetadata, destinationId, uploadId, emptyMap(), null
    )

    assertThat(
      Paths.get(
        bucketMetadata.path.toString(),
        MultipartStore.MULTIPARTS_FOLDER,
        uploadId,
        "$partNumber.part"
      ).toFile()
    ).exists()
    multipartStore.abortMultipartUpload(bucketMetadata, destinationId, uploadId)
  }

  @Test
  fun missingUploadPreparation() {
    val range = HttpRange.createByteRange(0, 0)
    val id = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val uploadId = UUID.randomUUID().toString()
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    val encryptionHeaders = encryptionHeaders()
    assertThatThrownBy {
      multipartStore.copyPart(
        bucketMetadata, id, range, "1",
        bucketMetadata, destinationId, uploadId,
        encryptionHeaders, null
      )
    }.isInstanceOf(IllegalStateException::class.java)
      .hasMessageStartingWith("Multipart Request was not prepared.")
  }

  @Test
  @Throws(IOException::class)
  fun multipartUploadPartsAreSortedNumerically() {
    val filename = UUID.randomUUID().toString()
    val id = managedId()

    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), filename, id, TEXT_PLAIN,
      storeHeaders(), TEST_OWNER, TEST_OWNER, NO_USER_METADATA, emptyMap(),
      StorageClass.STANDARD, null, null
    )
    val uploadId = multipartUpload.uploadId
    val multipartUploadInfo = multipartUploadInfo(multipartUpload)
    for (i in 1..10) {
      val tempFile = Files.createTempFile("", "")
      ByteArrayInputStream(("$i\n").toByteArray(StandardCharsets.UTF_8))
        .transferTo(Files.newOutputStream(tempFile))

      multipartStore.putPart(
        metadataFrom(TEST_BUCKET_NAME), id, uploadId, i.toString(),
        tempFile, emptyMap()
      )
    }
    multipartStore.completeMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME), filename, id, uploadId,
      getParts(10), emptyMap(), multipartUploadInfo, "location"
    )
    val s = objectStore.getS3ObjectMetadata(metadataFrom(TEST_BUCKET_NAME), id, null)
      .dataPath
      .toFile()
      .readLines()

    assertThat(s).containsExactlyElementsOf((1..10).map { "$it" })
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

  private fun multipartUploadInfo(multipartUpload: MultipartUpload?) = MultipartUploadInfo(
    multipartUpload,
    "application/octet-stream",
    mapOf(),
    mapOf(),
    mapOf(),
    "bucket",
    null,
    "checksum",
    ChecksumAlgorithm.CRC32
  )

  companion object {
    private val idCache: MutableList<UUID> = Collections.synchronizedList(arrayListOf<UUID>())

    @JvmStatic
    @AfterAll
    fun afterAll() {
      assertThat(idCache).isEmpty()
    }
  }
}
