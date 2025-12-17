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
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.Initiator
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import org.apache.http.entity.ContentType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import java.io.File
import java.util.Date
import java.util.UUID

internal abstract class StoreTestBase {
  @Autowired
  private lateinit var rootFolder: File

  protected fun metadataFrom(bucketName: String): BucketMetadata {
    BUCKET_NAMES.add(bucketName)
    return BucketMetadata(
      bucketName,
      Date().toString(),
      null,
      null,
      null,
      ObjectOwnership.BUCKET_OWNER_ENFORCED,
      rootFolder.toPath().resolve(bucketName),
      "us-east-1",
      null,
      null,
    )
  }

  protected fun encryptionHeaders(): Map<String, String> = mapOf(
    AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION to TEST_ENC_TYPE,
    AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID to TEST_ENC_KEY
  )

  protected fun storeHeaders(): Map<String, String> = mapOf(HttpHeaders.CONTENT_ENCODING to ENCODING_GZIP)

  companion object {
    const val TEST_BUCKET_NAME = "test-bucket"
    const val TEST_FILE_PATH = "src/test/resources/sampleFile.txt"

    val NO_USER_METADATA: Map<String, String> = emptyMap()
    val NO_ENCRYPTION_HEADERS: Map<String, String> = emptyMap()
    val NO_TAGS: List<Tag> = emptyList()
    val NO_CHECKSUMTYPE: ChecksumType? = null
    val NO_CHECKSUM: String? = null
    val NO_CHECKSUM_ALGORITHM: ChecksumAlgorithm? = null
    const val TEST_ENC_TYPE = "aws:kms"

    val TEST_ENC_KEY: String = "aws:kms${UUID.randomUUID()}"

    val TEXT_PLAIN: String = ContentType.TEXT_PLAIN.toString()
    const val ENCODING_GZIP = "gzip"
    val NO_PREFIX: String? = null
    const val DEFAULT_CONTENT_TYPE = MediaType.APPLICATION_OCTET_STREAM_VALUE
    val TEST_OWNER: Owner = Owner("123")
    val TEST_INITIATOR: Initiator = Initiator("s3-mock-file-store", "123")

    val BUCKET_NAMES = mutableSetOf<String>()
  }
}
