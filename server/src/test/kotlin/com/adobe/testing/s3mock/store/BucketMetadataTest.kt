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
import com.adobe.testing.s3mock.dto.AbortIncompleteMultipartUpload
import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.BucketType
import com.adobe.testing.s3mock.dto.DataRedundancy
import com.adobe.testing.s3mock.dto.DefaultRetention
import com.adobe.testing.s3mock.dto.LifecycleExpiration
import com.adobe.testing.s3mock.dto.LifecycleRule
import com.adobe.testing.s3mock.dto.LifecycleRuleAndOperator
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.LocationType
import com.adobe.testing.s3mock.dto.Mode
import com.adobe.testing.s3mock.dto.NoncurrentVersionExpiration
import com.adobe.testing.s3mock.dto.NoncurrentVersionTransition
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectLockEnabled
import com.adobe.testing.s3mock.dto.ObjectLockRule
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag
import com.adobe.testing.s3mock.dto.Transition
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.dto.VersioningConfiguration.MFADelete
import com.adobe.testing.s3mock.dto.VersioningConfiguration.Status.ENABLED
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.nio.file.Path
import java.time.Instant
import java.util.UUID

class BucketMetadataTest {

  @Test
  fun testDeserialization(testInfo: TestInfo) {
    val iut = DtoTestUtil.deserializeJSON(BucketMetadata::class.java, testInfo)

    assertThat(iut).isNotNull()
    assertThat(iut.name).isEqualTo("my-test-bucket")
    assertThat(iut.creationDate).isEqualTo("2025-09-12T22:42:41.844Z")
    assertThat(iut.versioningConfiguration).isEqualTo(versioningConfiguration())
    assertThat(iut.objectLockConfiguration).isEqualTo(objectLockConfiguration())
    assertThat(iut.bucketLifecycleConfiguration).isEqualTo(bucketLifecycleConfiguration())
    assertThat(iut.objectOwnership).isEqualTo(ObjectOwnership.BUCKET_OWNER_ENFORCED)
    assertThat(iut.path).isEqualTo(Path.of("/s3mock-test/my-test-bucket/"))
    assertThat(iut.bucketRegion).isEqualTo("us-east-1")
    assertThat(iut.bucketInfo).isEqualTo(bucketInfo())
    assertThat(iut.locationInfo).isEqualTo(locationInfo())
    assertThat(iut.objects).isNotNull()
    assertThat(iut.objects).containsOnlyKeys("my-file")
    assertThat(iut.objects["my-file"]).isEqualTo(UUID.fromString("eb9cfbf2-571e-41c1-a0b8-91c1495b5529"))
  }

  @Test
  fun testSerialization(testInfo: TestInfo) {

    val iut = BucketMetadata(
      name = "testputgetobject-withmultipleversions-763480000",
      creationDate = "2025-09-12T22:50:48.934Z",
      versioningConfiguration = versioningConfiguration(),
      objectLockConfiguration = objectLockConfiguration(),
      bucketLifecycleConfiguration = bucketLifecycleConfiguration(),
      objectOwnership = ObjectOwnership.BUCKET_OWNER_ENFORCED,
      path = Path.of("/s3mock-test/testputgetobject-withmultipleversions-763480000/"),
      bucketRegion = "us-east-1",
      bucketInfo = bucketInfo(),
      locationInfo = locationInfo(),
      objects = mutableMapOf("src/test/resources/sampleFile_large.txt" to UUID.fromString("c6fe9dd9-2c83-4f34-a934-5da6d7d4ea2c"))
    )
    DtoTestUtil.serializeAndAssertJSON(iut, testInfo)
  }

  private fun versioningConfiguration(): VersioningConfiguration = VersioningConfiguration(MFADelete.ENABLED, ENABLED)

  private fun objectLockConfiguration(): ObjectLockConfiguration = ObjectLockConfiguration(
    ObjectLockEnabled.ENABLED,
    ObjectLockRule(
      DefaultRetention(
        1,
        Mode.GOVERNANCE,
        null
      )
    )
  )

  private fun bucketLifecycleConfiguration(): BucketLifecycleConfiguration = BucketLifecycleConfiguration(
    listOf(
      LifecycleRule(
        abortIncompleteMultipartUpload = AbortIncompleteMultipartUpload(2),
        expiration = LifecycleExpiration(
          Instant.parse("2025-09-12T22:42:41.844Z"),
          2,
          true
        ),
        filter = LifecycleRuleFilter(
          2,
          1,
          "myprefix/",
          listOf(
            Tag("key1", "value1")
          ),
          LifecycleRuleAndOperator(
            3,
            2,
            "myprefix/1",
            listOf(
              Tag("key2", "value2")
            )
          )
        ),
        id = "put-bucket-lifecycle-is-successful--get-bucket-lif-431062000",
        noncurrentVersionExpiration = NoncurrentVersionExpiration(1, 2),
        noncurrentVersionTransitions = listOf(NoncurrentVersionTransition(2, 2, StorageClass.STANDARD_IA)),
        status = LifecycleRule.Status.ENABLED,
        transitions = listOf(
          Transition(
            Instant.parse("2025-09-12T22:42:41.844Z"),
            2,
            StorageClass.STANDARD_IA
          )
        )
      )
    )
  )

  private fun bucketInfo(): BucketInfo = BucketInfo(DataRedundancy.SINGLE_AVAILABILITY_ZONE, BucketType.DIRECTORY)

  private fun locationInfo(): LocationInfo = LocationInfo("us-east-1", LocationType.AVAILABILITY_ZONE)
}
