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
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.CompletedPart
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Part
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.util.DigestUtil
import com.adobe.testing.s3mock.util.HeaderUtil
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
import org.springframework.http.HttpRange
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.security.MessageDigest
import java.util.Collections
import java.util.Date
import java.util.UUID
import kotlin.io.path.outputStream

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockitoBean(types = [KmsKeyStore::class, BucketStore::class])
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
  fun `createMultipartUpload creates the correct set of folders`() {
    val fileName = "aFile"
    val id = managedId()
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    Paths.get(
      rootFolder.absolutePath,
      TEST_BUCKET_NAME,
      MultipartStore.MULTIPARTS_FOLDER,
      uploadId.toString()
    )
      .toFile().also {
        assertThat(it)
          .exists()
          .isDirectory()
      }

    multipartStore.abortMultipartUpload(bucket, id, uploadId)
  }

  @Test
  @Throws(IOException::class)
  fun `PUT part creates the correct set of folders and files`() {
    val fileName = "PartFile"
    val partNumber = 1
    val id = managedId()
    val part = "Part1"
    val tempFile = Files.createTempFile("", "")
    part.toByteArray().inputStream().transferTo(tempFile.outputStream())
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName, id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    multipartStore.putPart(
      bucket,
      id,
      uploadId,
      partNumber,
      tempFile,
      NO_ENCRYPTION_HEADERS
    )
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME,
        MultipartStore.MULTIPARTS_FOLDER,
        uploadId.toString(),
        "$partNumber.part"
      ).toFile()
    ).exists()

    multipartStore.abortMultipartUpload(bucket, id, uploadId)
  }

  @Test
  @Throws(IOException::class)
  fun `completeMultipartUpload creates the correct set of folders and files`() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    multipartStore
      .putPart(
        bucket,
        id,
        uploadId,
        1,
        tempFile1,
        NO_ENCRYPTION_HEADERS
      )
    multipartStore
      .putPart(
        bucket,
        id,
        uploadId,
        2,
        tempFile2,
        NO_ENCRYPTION_HEADERS
      )

    val result =
      multipartStore.completeMultipartUpload(
        bucket,
        fileName, id,
        uploadId,
        getParts(2),
        NO_ENCRYPTION_HEADERS,
        multipartUploadInfo,
        "location",
        NO_CHECKSUM,
        NO_CHECKSUMTYPE,
        NO_CHECKSUM_ALGORITHM,
      )
    val md5 = MessageDigest.getInstance("MD5")
    val allMd5s = md5.digest("Part1".toByteArray()) + md5.digest("Part2".toByteArray())

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
    assertThat(result.etag).isEqualTo("\"${md5.digest(allMd5s).joinToString("") {"%02x".format(it)} }-2\"")
  }

  @Test
  @Throws(IOException::class)
  fun `MultipartUpload creates an object in S3Mock`() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())

    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        1,
        tempFile1,
      NO_ENCRYPTION_HEADERS
      )
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        2,
        tempFile2,
      NO_ENCRYPTION_HEADERS
      )

    multipartStore.completeMultipartUpload(
      bucket,
      fileName,
      id,
      uploadId,
      getParts(2),
      NO_ENCRYPTION_HEADERS,
      multipartUploadInfo,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )

    objectStore.getS3ObjectMetadata(bucket, id, null).also {
      assertThat(it!!.size).isEqualTo("10")
      assertThat(it.contentType).isEqualTo(MediaType.APPLICATION_OCTET_STREAM.toString())
    }
  }

  @Test
  @Throws(IOException::class)
  fun `MultipartUpload creates an object with metadata and encryption headers in S3Mock`() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())

    val userMetadata = mapOf("${HeaderUtil.HEADER_X_AMZ_META_PREFIX}test" to "test")
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      userMetadata,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        1,
        tempFile1,
      NO_ENCRYPTION_HEADERS
      )
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        2,
        tempFile2,
      NO_ENCRYPTION_HEADERS
      )

    multipartStore.completeMultipartUpload(
      bucket,
      fileName,
      id,
      uploadId,
      getParts(2),
      encryptionHeaders(),
      multipartUploadInfo,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )

    objectStore.getS3ObjectMetadata(bucket, id, null).also {
      assertThat(it!!.userMetadata).isEqualTo(userMetadata)
      assertThat(it.encryptionHeaders).isEqualTo(encryptionHeaders())
    }
  }

  @Test
  @Throws(IOException::class)
  fun `MultipartUpload creates an object with checksums in S3Mock`() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val checksumAlgorithm = ChecksumAlgorithm.CRC32
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())
    val checksum = DigestUtil.checksumMultipart(listOf(tempFile1, tempFile2), DefaultChecksumAlgorithm.CRC32)
    val checksum1 = DigestUtil.checksumFor(tempFile1, DefaultChecksumAlgorithm.CRC32)
    val checksum2 = DigestUtil.checksumFor(tempFile2, DefaultChecksumAlgorithm.CRC32)

    val userMetadata = mapOf("${HeaderUtil.HEADER_X_AMZ_META_PREFIX}test" to "test")
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      userMetadata,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      ChecksumType.COMPOSITE,
      checksumAlgorithm,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        1,
        tempFile1,
      NO_ENCRYPTION_HEADERS
      )
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        2,
        tempFile2,
      NO_ENCRYPTION_HEADERS
      )

    multipartStore.completeMultipartUpload(
      bucket,
      fileName,
      id,
      uploadId,
      listOf(
        CompletedPart(
          checksum1,
          null,
          null,
          null,
          null,
          null,
          1
          ),
        CompletedPart(
          checksum2,
          null,
          null,
          null,
          null,
          null,
          2
          ),
      ),
      encryptionHeaders(),
      multipartUploadInfo,
      "location",
      checksum,
      ChecksumType.COMPOSITE,
      ChecksumAlgorithm.CRC32,
    )
  }

  @Test
  @Throws(IOException::class)
  fun `MultipartUpload fails when overall checksum does not match`() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())

    val checksumAlgorithm = ChecksumAlgorithm.CRC32
    val checksum1 = DigestUtil.checksumFor(tempFile1, DefaultChecksumAlgorithm.CRC32)
    val checksum2 = DigestUtil.checksumFor(tempFile2, DefaultChecksumAlgorithm.CRC32)

    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      ChecksumType.COMPOSITE,
      checksumAlgorithm,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)

    multipartStore.putPart(bucket, id, uploadId, 1, tempFile1, NO_ENCRYPTION_HEADERS)
    multipartStore.putPart(bucket, id, uploadId, 2, tempFile2, NO_ENCRYPTION_HEADERS)

    // Provide wrong overall checksum to trigger verification failure
    val wrongOverallChecksum = "AAAAAAAA" // invalid CRC32 base64

    assertThatThrownBy {
      multipartStore.completeMultipartUpload(
        bucket,
        fileName,
        id,
        uploadId,
        listOf(
          CompletedPart(checksum1, null, null, null, null, null, 1),
          CompletedPart(checksum2, null, null, null, null, null, 2),
        ),
        NO_ENCRYPTION_HEADERS,
        multipartUploadInfo,
        "location",
        wrongOverallChecksum,
        ChecksumType.COMPOSITE,
        ChecksumAlgorithm.CRC32,
      )
    }.isInstanceOf(S3Exception::class.java)
  }

  @Test
  @Throws(IOException::class)
  fun `MultipartUpload creates an object with checksums in S3Mock, missing checksum in completeMultipartUpload`() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val checksumAlgorithm = ChecksumAlgorithm.CRC32
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())
    val checksum = DigestUtil.checksumMultipart(listOf(tempFile1, tempFile2), DefaultChecksumAlgorithm.CRC32)

    val userMetadata = mapOf("${HeaderUtil.HEADER_X_AMZ_META_PREFIX}test" to "test")
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      userMetadata,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      ChecksumType.COMPOSITE,
      checksumAlgorithm,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        1,
        tempFile1,
      NO_ENCRYPTION_HEADERS
      )
    multipartStore.putPart(
      bucket,
        id,
        uploadId,
        2,
        tempFile2,
      NO_ENCRYPTION_HEADERS
      )

    assertThatThrownBy {
      multipartStore.completeMultipartUpload(
        bucket,
        fileName,
        id,
        uploadId,
        getParts(2),
        encryptionHeaders(),
        multipartUploadInfo,
        "location",
        checksum,
        ChecksumType.COMPOSITE,
        ChecksumAlgorithm.CRC32,
      )
    }.isInstanceOf(S3Exception::class.java)
      .hasMessageContaining("The upload was created using a crc32 checksum. The complete request must include the " +
        "checksum for each part. It was missing for part 1 in the request.")
  }

  private fun getParts(n: Int): List<CompletedPart> = (1..n).map {
    CompletedPart(
      null,
      null,
      null,
      null,
      null,
      null,
      it
    )
  }

  @Test
  @Throws(IOException::class)
  fun returnsValidPartsFromMultipart() {
    val fileName = "PartFile"
    val id = managedId()
    val part1 = "Part1"
    val part2 = "Part2"
    val tempFile1 = Files.createTempFile("", "")
    part1.toByteArray().inputStream().transferTo(tempFile1.outputStream())
    val tempFile2 = Files.createTempFile("", "")
    part2.toByteArray().inputStream().transferTo(tempFile2.outputStream())

    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)

    multipartStore.putPart(
      bucket,
      id,
      uploadId,
      1,
      tempFile1,
      NO_ENCRYPTION_HEADERS
    )
    multipartStore.putPart(
      bucket,
      id,
      uploadId,
      2,
      tempFile2,
      NO_ENCRYPTION_HEADERS
    )

    multipartStore.getMultipartUploadParts(bucket, id, uploadId).also {
      assertThat(it).hasSize(2)

      val expectedPart1 = prepareExpectedPart(1, it[0].lastModified, part1)
      val expectedPart2 = prepareExpectedPart(2, it[1].lastModified, part2)

      assertThat(it[0]).isEqualTo(expectedPart1)
      assertThat(it[1]).isEqualTo(expectedPart2)
    }


    multipartStore.abortMultipartUpload(bucket, id, uploadId)
  }

  private fun prepareExpectedPart(partNumber: Int, lastModified: Date, content: String): Part {
    val md5 = MessageDigest.getInstance("MD5")
    return Part(
      partNumber,
      md5.digest(content.toByteArray()).joinToString("") { "%02x".format(it) },
      lastModified,
      content.toByteArray().size.toLong()
    )
  }

  @Test
  @Throws(IOException::class)
  fun deletesTemporaryMultipartUploadFolder() {
    val fileName = "PartFile"
    val id = managedId()
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    val tempFile = Files.createTempFile("", "")
    "Part1".toByteArray().inputStream().transferTo(tempFile.outputStream())
    multipartStore.putPart(
      bucket,
      id,
      uploadId,
      1,
      tempFile,
      emptyMap()
    )

    multipartStore.completeMultipartUpload(
      bucket,
      fileName,
      id,
      uploadId,
      getParts(1),
      NO_ENCRYPTION_HEADERS,
      multipartUploadInfo,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )

    assertThat(
      Paths.get(rootFolder.absolutePath, TEST_BUCKET_NAME, fileName, uploadId.toString())
        .toFile()
    ).doesNotExist()
  }

  @Test
  fun listsMultipartUploads() {
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    assertThat(multipartStore.listMultipartUploads(bucket, NO_PREFIX)).isEmpty()

    val fileName = "PartFile"
    val id = managedId()
    val multipartUpload = multipartStore
      .createMultipartUpload(
        bucket,
        fileName,
        id,
        DEFAULT_CONTENT_TYPE,
        storeHeaders(),
        TEST_OWNER,
        TEST_INITIATOR,
        NO_USER_METADATA,
        NO_ENCRYPTION_HEADERS,
        NO_TAGS,
        StorageClass.STANDARD,
        NO_CHECKSUMTYPE,
        NO_CHECKSUM_ALGORITHM,
      )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    val uploads = multipartStore.listMultipartUploads(bucket, NO_PREFIX)
    assertThat(uploads).hasSize(1)
    uploads.first().also {
      assertThat(it).isEqualTo(multipartUpload)
      // and some specific sanity checks
      assertThat(it.uploadId).isEqualTo(uploadId.toString())
      assertThat(it.key).isEqualTo(fileName)
    }

    multipartStore.completeMultipartUpload(
      bucket,
      fileName,
      id,
      uploadId,
      getParts(0),
      NO_ENCRYPTION_HEADERS,
      multipartUploadInfo,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )

    assertThat(multipartStore.listMultipartUploads(bucket, NO_PREFIX)).isEmpty()
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
        bucketMetadata1,
        fileName1,
        id1,
        DEFAULT_CONTENT_TYPE,
        storeHeaders(),
        TEST_OWNER,
        TEST_INITIATOR,
        NO_USER_METADATA,
        NO_ENCRYPTION_HEADERS,
        NO_TAGS,
        StorageClass.STANDARD,
        NO_CHECKSUMTYPE,
        NO_CHECKSUM_ALGORITHM,
      )
    val uploadId1 = UUID.fromString(multipartUpload1.uploadId)
    val multipartUploadInfo1 = multipartStore.getMultipartUploadInfo(bucketMetadata1, uploadId1)
    val fileName2 = "PartFile2"
    val id2 = managedId()
    val multipartUpload2 = multipartStore
      .createMultipartUpload(
        bucketMetadata2,
        fileName2,
        id2,
        DEFAULT_CONTENT_TYPE,
        storeHeaders(),
        TEST_OWNER,
        TEST_INITIATOR,
        NO_USER_METADATA,
        NO_ENCRYPTION_HEADERS,
        NO_TAGS,
        StorageClass.STANDARD,
        NO_CHECKSUMTYPE,
        NO_CHECKSUM_ALGORITHM,
      )
    val uploadId2 = UUID.fromString(multipartUpload2.uploadId)
    val multipartUploadInfo2 = multipartStore.getMultipartUploadInfo(bucketMetadata2, uploadId2)
    multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX).also {
      assertThat(it).hasSize(1)
      it[0].also {
        assertThat(it).isEqualTo(multipartUpload1)
        // and some specific sanity checks
        assertThat(it.uploadId).isEqualTo(uploadId1.toString())
        assertThat(it.key).isEqualTo(fileName1)
      }
    }

    multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX).also {
      assertThat(it).hasSize(1)
      it[0].also {
        assertThat(it).isEqualTo(multipartUpload2)
        // and some specific sanity checks
        assertThat(it.uploadId).isEqualTo(uploadId2.toString())
        assertThat(it.key).isEqualTo(fileName2)
      }
    }

    multipartStore.completeMultipartUpload(
      bucketMetadata1,
      fileName1,
      id1,
      uploadId1,
      getParts(0),
      NO_ENCRYPTION_HEADERS,
      multipartUploadInfo1,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    multipartStore.completeMultipartUpload(
      bucketMetadata2,
      fileName2,
      id2,
      uploadId2,
      getParts(0),
      NO_ENCRYPTION_HEADERS,
      multipartUploadInfo2,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )

    assertThat(multipartStore.listMultipartUploads(bucketMetadata1, NO_PREFIX)).isEmpty()
    assertThat(multipartStore.listMultipartUploads(bucketMetadata2, NO_PREFIX)).isEmpty()
  }

  @Test
  @Throws(IOException::class)
  fun abortMultipartUpload() {
    val bucket = metadataFrom(TEST_BUCKET_NAME)
    assertThat(multipartStore.listMultipartUploads(bucket, NO_PREFIX)).isEmpty()

    val fileName = "PartFile"
    val id = managedId()
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      fileName,
      id,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val tempFile = Files.createTempFile("", "")
    "Part1".toByteArray().inputStream().transferTo(tempFile.outputStream())
    multipartStore.putPart(
      bucket, id, uploadId, 1,
      tempFile, emptyMap()
    )
    assertThat(multipartStore.listMultipartUploads(bucket, NO_PREFIX)).hasSize(1)

    multipartStore.abortMultipartUpload(bucket, id, uploadId)

    assertThat(multipartStore.listMultipartUploads(bucket, NO_PREFIX)).isEmpty()
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
        TEST_BUCKET_NAME, fileName, uploadId.toString()
      ).toFile()
    ).doesNotExist()
  }

  @Test
  @Throws(IOException::class)
  fun copyPart() {
    val sourceFile = UUID.randomUUID().toString()
    val sourceId = managedId()
    val targetFile = UUID.randomUUID().toString()
    val partNumber = 1
    val destinationId = managedId()

    val contentBytes = UUID.randomUUID().toString().toByteArray()
    val tempFile = Files.createTempFile("", "")
    contentBytes.inputStream().transferTo(tempFile.outputStream())
    objectStore.storeS3ObjectMetadata(
      metadataFrom(TEST_BUCKET_NAME),
      sourceId,
      sourceFile,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      tempFile,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      null,
      NO_TAGS,
      NO_CHECKSUM_ALGORITHM,
      NO_CHECKSUM,
      Owner.DEFAULT_OWNER,
      StorageClass.STANDARD,
      ChecksumType.COMPOSITE
    )

    val multipartUpload = multipartStore.createMultipartUpload(
      metadataFrom(TEST_BUCKET_NAME),
      targetFile,
      destinationId,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)

    val range = HttpRange.createByteRange(0, contentBytes.size.toLong())
    multipartStore.copyPart(
      metadataFrom(TEST_BUCKET_NAME),
      sourceId,
      range,
      partNumber,
      metadataFrom(TEST_BUCKET_NAME),
      destinationId,
      uploadId,
      NO_ENCRYPTION_HEADERS,
      null
    )
    assertThat(
      Paths.get(
        rootFolder.absolutePath,
        TEST_BUCKET_NAME,
        MultipartStore.MULTIPARTS_FOLDER,
        uploadId.toString(),
        "$partNumber.part"
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
    val partNumber = 1
    val destinationId = managedId()
    val contentBytes = UUID.randomUUID().toString().toByteArray()
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    val tempFile = Files.createTempFile("", "")
    contentBytes.inputStream().transferTo(tempFile.outputStream())
    objectStore.storeS3ObjectMetadata(
      bucketMetadata,
      sourceId,
      sourceFile,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      tempFile,
      NO_USER_METADATA,
      emptyMap(),
      null,
      NO_TAGS,
      NO_CHECKSUM_ALGORITHM,
      NO_CHECKSUM,
      Owner.DEFAULT_OWNER,
      StorageClass.STANDARD,
      ChecksumType.COMPOSITE
    )

    val multipartUpload = multipartStore.createMultipartUpload(
      bucketMetadata,
      targetFile,
      destinationId,
      DEFAULT_CONTENT_TYPE,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)

    multipartStore.copyPart(
      bucketMetadata,
      sourceId,
      null,
      partNumber,
      bucketMetadata,
      destinationId,
      uploadId,
      NO_ENCRYPTION_HEADERS,
      null
    )

    assertThat(
      Paths.get(
        bucketMetadata.path.toString(),
        MultipartStore.MULTIPARTS_FOLDER,
        uploadId.toString(),
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
    val uploadId = UUID.randomUUID()
    val bucketMetadata = metadataFrom(TEST_BUCKET_NAME)
    val encryptionHeaders = encryptionHeaders()
    assertThatThrownBy {
      multipartStore.copyPart(
        bucketMetadata,
        id,
        range,
        1,
        bucketMetadata,
        destinationId,
        uploadId,
        encryptionHeaders,
        null
      )
    }.isInstanceOf(IllegalStateException::class.java)
      .hasMessageStartingWith("Multipart Request was not prepared.")
  }

  @Test
  @Throws(IOException::class)
  fun multipartUploadPartsAreSortedNumerically() {
    val filename = UUID.randomUUID().toString()
    val id = managedId()

    val bucket = metadataFrom(TEST_BUCKET_NAME)
    val multipartUpload = multipartStore.createMultipartUpload(
      bucket,
      filename,
      id,
      TEXT_PLAIN,
      storeHeaders(),
      TEST_OWNER,
      TEST_INITIATOR,
      NO_USER_METADATA,
      NO_ENCRYPTION_HEADERS,
      NO_TAGS,
      StorageClass.STANDARD,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val uploadId = UUID.fromString(multipartUpload.uploadId)
    val multipartUploadInfo = multipartStore.getMultipartUploadInfo(bucket, uploadId)
    for (i in 1..10) {
      val tempFile = Files.createTempFile("", "")
      ("$i\n").toByteArray(StandardCharsets.UTF_8).inputStream().transferTo(tempFile.outputStream())

      multipartStore.putPart(
        bucket, id, uploadId, i,
        tempFile, emptyMap()
      )
    }
    multipartStore.completeMultipartUpload(
      bucket,
      filename,
      id,
      uploadId,
      getParts(10),
      NO_ENCRYPTION_HEADERS,
      multipartUploadInfo,
      "location",
      NO_CHECKSUM,
      NO_CHECKSUMTYPE,
      NO_CHECKSUM_ALGORITHM,
    )
    val s = objectStore.getS3ObjectMetadata(bucket, id, null)!!.dataPath
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
    val snapshot = idCache.toList()
    snapshot.forEach { id ->
      BUCKET_NAMES.forEach { bucketName ->
        objectStore.deleteObject(metadataFrom(bucketName), id, null)
      }
    }
    idCache.removeAll(snapshot)

    BUCKET_NAMES.forEach { bucket ->
      val bucketMetadata = metadataFrom(bucket)
      multipartStore.listMultipartUploads(bucketMetadata, NO_PREFIX).forEach {
        multipartStore.abortMultipartUpload(bucketMetadata, UUID.randomUUID(), UUID.fromString(it.uploadId))
      }
    }
  }

  companion object {
    private val idCache: MutableList<UUID> = Collections.synchronizedList(arrayListOf<UUID>())

    @JvmStatic
    @AfterAll
    fun afterAll() {
      assertThat(idCache).isEmpty()
    }
  }
}
