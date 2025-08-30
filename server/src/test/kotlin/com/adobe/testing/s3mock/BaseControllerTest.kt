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
package com.adobe.testing.s3mock

import com.adobe.testing.s3mock.dto.Bucket
import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.ErrorResponse
import com.adobe.testing.s3mock.dto.LegalHold
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.Owner
import com.adobe.testing.s3mock.dto.Retention
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.store.BucketMetadata
import com.adobe.testing.s3mock.store.S3ObjectMetadata
import com.adobe.testing.s3mock.util.AwsHttpHeaders
import com.ctc.wstx.api.WstxOutputProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.UUID

internal abstract class BaseControllerTest {
  companion object {
    val MAPPER: XmlMapper = XmlMapper.builder()
      .findAndAddModules()
      .enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
      .enable(ToXmlGenerator.Feature.AUTO_DETECT_XSI_TYPE)
      .enable(FromXmlParser.Feature.AUTO_DETECT_XSI_TYPE)
      .build()

    init {
      MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      MAPPER.factory.xmlOutputFactory
        .setProperty(WstxOutputProperties.P_USE_DOUBLE_QUOTES_IN_XML_DECL, true)
    }

    fun from(e: S3Exception): ErrorResponse {
      return ErrorResponse(
        e.code,
        e.message,
        null,
        null
      )
    }

    fun bucketMetadata(
      name: String = TEST_BUCKET_NAME,
      creationDate: String = Instant.now().toString(),
      path: Path = Paths.get("/tmp/foo/1"),
      bucketRegion: String = "us-east-1",
      versioningConfiguration: VersioningConfiguration? = null,
      bucketInfo: BucketInfo? = null,
      locationInfo: LocationInfo? = null
    ): BucketMetadata {
      return BucketMetadata(
        name,
        creationDate,
        versioningConfiguration,
        null,
        null,
        null,
        path,
        bucketRegion,
        bucketInfo,
        locationInfo,
      )
    }

    fun s3ObjectEncrypted(
      key: String,
      digest: String = UUID.randomUUID().toString(),
      encryption: String?,
      encryptionKey: String?
    ): S3ObjectMetadata {
      return s3ObjectMetadata(
        key, digest, encryption, encryptionKey,
      )
    }

    fun s3ObjectMetadata(
      key: String,
      digest: String = UUID.randomUUID().toString(),
      encryption: String? = null,
      encryptionKey: String? = null,
      retention: Retention? = null,
      tags: List<Tag>? = null,
      legalHold: LegalHold? = null,
      versionId: String? = null,
      checksum: String? = null,
      checksumType: ChecksumType? = ChecksumType.FULL_OBJECT,
      checksumAlgorithm: ChecksumAlgorithm? = null,
      userMetadata: Map<String, String>? = null,
      storeHeaders: Map<String, String>? = null,
    ): S3ObjectMetadata {
      return S3ObjectMetadata(
        UUID.randomUUID(),
        key,
        Path.of(UPLOAD_FILE_NAME).toFile().length().toString(),
        "1234",
        digest,
        "text/plain",
        1L,
        Path.of(UPLOAD_FILE_NAME),
        userMetadata,
        tags,
        legalHold,
        retention,
        Owner.DEFAULT_OWNER,
        storeHeaders,
        encryptionHeaders(encryption, encryptionKey),
        checksumAlgorithm,
        checksum,
        null,
        null,
        versionId,
        false,
        checksumType
      )
    }

    private fun encryptionHeaders(encryption: String?, encryptionKey: String?): Map<String, String> {
      val pairs = mutableListOf<Pair<String, String>>()
      if (encryption != null) {
        pairs.add(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION to encryption)
      }
      if(encryptionKey!= null)  {
        pairs.add(AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID to encryptionKey)
      }

      return pairs.associate { it.first to it.second }
    }
    val TEST_OWNER = Owner("s3-mock-file-store", "123")
    val TEST_BUCKETMETADATA = bucketMetadata()
    const val UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt"

    const val TEST_BUCKET_NAME = "test-bucket"
    val CREATION_DATE = Instant.now().toString()
    const val BUCKET_REGION = "us-west-2"
    val BUCKET_PATH: Path = Paths.get("/tmp/foo/1")
    val TEST_BUCKET = Bucket(
      TEST_BUCKET_NAME,
      BUCKET_REGION,
      CREATION_DATE,
      BUCKET_PATH
    )
  }
}
