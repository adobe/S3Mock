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

import com.adobe.testing.s3mock.DtoTestUtil
import com.adobe.testing.s3mock.dto.AccessControlPolicy
import com.adobe.testing.s3mock.dto.CanonicalUser
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Grant
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.Owner.Companion.DEFAULT_OWNER
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.nio.file.Path
import java.time.Instant
import java.util.UUID

class S3ObjectMetadataTest {

  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(
      S3ObjectMetadata::class.java, testInfo
    )
    assertThat(iut).isNotNull()
    assertThat(iut.id).isEqualTo(UUID.fromString("c6fe9dd9-2c83-4f34-a934-5da6d7d4ea2c"))
    assertThat(iut.key).isEqualTo("src/test/resources/sampleFile_large.txt")
    assertThat(iut.size).isEqualTo("63839")
    assertThat(iut.lastModified).isEqualTo(1757717449038)
    assertThat(iut.etag).isEqualTo("\"029c3c4705e220ff1d8dc080e82847bd\"")
    assertThat(iut.contentType).isEqualTo("text/plain")
    assertThat(iut.userMetadata).isEqualTo(
      mapOf(
        "key1" to "value1",
        "key2" to "value2"
      )
    )
    assertThat(iut.legalHold).isEqualTo(LegalHold(LegalHold.Status.ON))
    assertThat(iut.retention).isEqualTo(Retention(Mode.GOVERNANCE, Instant.parse("2025-09-12T22:50:49.038Z")))
    assertThat(iut.owner).isNotNull()
    assertThat(iut.owner.id).isEqualTo("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")
    assertThat(iut.storeHeaders).isEqualTo(
      mapOf(
        "Expires" to "123",
        "Content-Language" to "English"
      )
    )
    assertThat(iut.encryptionHeaders).isEqualTo(
      mapOf(
        "x-amz-server-side-encryption" to "AES256",
        "x-amz-server-side-encryption-aws-kms-key-id" to "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
        "x-amz-server-side-encryption-customer-algorithm" to "AES256",
        "x-amz-server-side-encryption-customer-key" to "",
        "x-amz-server-side-encryption-customer-key-MD5" to "hA+Z6eBBuIOwFTT4Eez14U3O2K8="
      )
    )
    assertThat(iut.checksumAlgorithm).isEqualTo(ChecksumAlgorithm.SHA1)
    assertThat(iut.checksum).isEqualTo("hA+Z6eBBuIOwFTT4Eez14U3O2K8=")
    assertThat(iut.storageClass).isEqualTo(StorageClass.EXPRESS_ONEZONE)
    assertThat(iut.policy).isEqualTo(
      AccessControlPolicy(
        listOf(
          Grant(
            CanonicalUser(
              "Jane Doe",
              "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ef"
            ),
            Grant.Permission.FULL_CONTROL
          )
        ),
        DEFAULT_OWNER
      )
    )
    assertThat(iut.versionId).isEqualTo("796c301f-a714-4483-a0cc-9034f01c1a6d")
    assertThat(iut.deleteMarker).isFalse()
    assertThat(iut.checksumType).isEqualTo(ChecksumType.FULL_OBJECT)
    assertThat(iut.tags).isEqualTo(listOf(Tag("key1", "value1")))
  }

  @Test
  fun testSerialization(testInfo: TestInfo) {

    val iut = S3ObjectMetadata(
      UUID.fromString("c6fe9dd9-2c83-4f34-a934-5da6d7d4ea2c"),
      "src/test/resources/sampleFile_large.txt",
      "63839",
      "2025-09-12T22:50:49.070Z",
      "\"029c3c4705e220ff1d8dc080e82847bd\"",
      "text/plain",
      1757717449070L,
      Path.of("/s3mock-test/testputgetobject-withmultipleversions-763480000/c6fe9dd9-2c83-4f34-a934-5da6d7d4ea2c/f51669d1-84f9-42d8-90ae-34c9f3bb3944-binaryData"),
      mapOf(
        "key1" to "value1",
        "key2" to "value2"
      ),
      listOf(Tag("key1", "value1")),
      LegalHold(LegalHold.Status.ON),
      Retention(Mode.GOVERNANCE, Instant.parse("2025-09-12T22:50:49.038Z")),
      DEFAULT_OWNER,
      mapOf(
        "Expires" to "123",
        "Content-Language" to "English"
      ),
      mapOf(
        "x-amz-server-side-encryption" to "AES256",
        "x-amz-server-side-encryption-aws-kms-key-id" to "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
        "x-amz-server-side-encryption-customer-algorithm" to "AES256",
        "x-amz-server-side-encryption-customer-key" to "",
        "x-amz-server-side-encryption-customer-key-MD5" to "hA+Z6eBBuIOwFTT4Eez14U3O2K8="
      ),
      ChecksumAlgorithm.SHA1,
      "hA+Z6eBBuIOwFTT4Eez14U3O2K8=",
      StorageClass.EXPRESS_ONEZONE,
      AccessControlPolicy(
        listOf(
          Grant(
            CanonicalUser(
              "Jane Doe",
              "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ef"
            ),
            Grant.Permission.FULL_CONTROL
          )
        ),
        DEFAULT_OWNER
      ),
      "f51669d1-84f9-42d8-90ae-34c9f3bb3944",
      false,
      ChecksumType.FULL_OBJECT
    )
    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }
}
