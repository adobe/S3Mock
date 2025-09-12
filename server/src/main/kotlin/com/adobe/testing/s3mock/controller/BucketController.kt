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
package com.adobe.testing.s3mock.controller

import com.adobe.testing.S3Verified
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.CreateBucketConfiguration
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult
import com.adobe.testing.s3mock.dto.ListBucketResult
import com.adobe.testing.s3mock.dto.ListBucketResultV2
import com.adobe.testing.s3mock.dto.ListVersionsResult
import com.adobe.testing.s3mock.dto.LocationConstraint
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.Region
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.adobe.testing.s3mock.service.BucketService
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_OBJECT_LOCK_ENABLED
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_REGION
import com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_OWNERSHIP
import com.adobe.testing.s3mock.util.AwsHttpParameters.BUCKET_REGION
import com.adobe.testing.s3mock.util.AwsHttpParameters.CONTINUATION_TOKEN
import com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE
import com.adobe.testing.s3mock.util.AwsHttpParameters.FETCH_OWNER
import com.adobe.testing.s3mock.util.AwsHttpParameters.KEY_MARKER
import com.adobe.testing.s3mock.util.AwsHttpParameters.LIFECYCLE
import com.adobe.testing.s3mock.util.AwsHttpParameters.LIST_TYPE_V2
import com.adobe.testing.s3mock.util.AwsHttpParameters.LOCATION
import com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_BUCKETS
import com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIST_TYPE
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LOCATION
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_OBJECT_LOCK
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_VERSIONING
import com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_VERSIONS
import com.adobe.testing.s3mock.util.AwsHttpParameters.OBJECT_LOCK
import com.adobe.testing.s3mock.util.AwsHttpParameters.START_AFTER
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSIONING
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSIONS
import com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID_MARKER
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam

@CrossOrigin(origins = ["*"], exposedHeaders = ["*"])
@Controller
@RequestMapping($$"${com.adobe.testing.s3mock.controller.contextPath:}")
class BucketController(private val bucketService: BucketService) {
  // ===============================================================================================
  // /
  // ===============================================================================================
  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html).
   */
  @GetMapping(
    value = [
      "/"
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun listBuckets(
    @RequestParam(name = BUCKET_REGION, required = false) bucketRegion: Region?,
    @RequestParam(name = CONTINUATION_TOKEN, required = false) continuationToken: String?,
    @RequestParam(name = MAX_BUCKETS, defaultValue = "1000", required = false) maxBuckets: Int,
    @RequestParam(required = false) prefix: String?
  ): ResponseEntity<ListAllMyBucketsResult> =
    ResponseEntity.ok(
      bucketService.listBuckets(
        bucketRegion,
        continuationToken,
        maxBuckets,
        prefix
      )
    )

  // ===============================================================================================
  // /{bucketName:.+}
  // ===============================================================================================
  /**
   * Create a bucket if the name matches a simplified version of the bucket naming rules.
   * [API Reference Bucket Naming](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)
   */
  @PutMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      NOT_OBJECT_LOCK,
      NOT_LIFECYCLE,
      NOT_VERSIONING
    ]
  )
  @S3Verified(year = 2025)
  fun createBucket(
    @PathVariable bucketName: String,
    @RequestHeader(
      value = X_AMZ_BUCKET_OBJECT_LOCK_ENABLED,
      required = false,
      defaultValue = "false"
    ) objectLockEnabled: Boolean,
    @RequestHeader(
      value = X_AMZ_OBJECT_OWNERSHIP,
      required = false,
      defaultValue = "BucketOwnerEnforced"
    ) objectOwnership: ObjectOwnership,
    @RequestBody(required = false) createBucketRequest: CreateBucketConfiguration?
  ): ResponseEntity<Void> {
    bucketService.verifyBucketNameIsAllowed(bucketName)
    bucketService.verifyBucketDoesNotExist(bucketName)
    bucketService.createBucket(
      bucketName,
      objectLockEnabled,
      objectOwnership,
      regionFrom(createBucketRequest),
      createBucketRequest?.bucket,
      createBucketRequest?.location
    )
    return ResponseEntity.ok()
      .header(LOCATION, "/$bucketName")
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html).
   */
  @RequestMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    method = [
      RequestMethod.HEAD
    ]
  )
  @S3Verified(year = 2025)
  fun headBucket(@PathVariable bucketName: String): ResponseEntity<Void> {
    val bucketMetadata = bucketService.verifyBucketExists(bucketName)
    return ResponseEntity
      .ok()
      .header(X_AMZ_BUCKET_REGION, bucketMetadata.bucketRegion)
      .headers { it.setAll(bucketService.bucketLocationHeaders(bucketMetadata)) }
      .build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html).
   */
  @DeleteMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ], params = [
      NOT_LIFECYCLE
    ]
  )
  @S3Verified(year = 2025)
  fun deleteBucket(@PathVariable bucketName: String): ResponseEntity<Void> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.verifyBucketIsEmpty(bucketName)
    bucketService.deleteBucket(bucketName)
    return ResponseEntity.noContent().build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      VERSIONING,
      NOT_LIST_TYPE
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun getVersioningConfiguration(@PathVariable bucketName: String): ResponseEntity<VersioningConfiguration> {
    bucketService.verifyBucketExists(bucketName)
    val configuration = bucketService.getVersioningConfiguration(bucketName)
    return ResponseEntity.ok(configuration)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html).
   */
  @PutMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          VERSIONING
      }
  )
  @S3Verified(year = 2025)
  fun putVersioningConfiguration(
    @PathVariable bucketName: String,
    @RequestBody configuration: VersioningConfiguration
  ): ResponseEntity<Void> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.setVersioningConfiguration(bucketName, configuration)
    return ResponseEntity.ok().build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      OBJECT_LOCK,
      NOT_LIST_TYPE
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun getObjectLockConfiguration(@PathVariable bucketName: String): ResponseEntity<ObjectLockConfiguration> {
    bucketService.verifyBucketExists(bucketName)
    val configuration = bucketService.getObjectLockConfiguration(bucketName)
    return ResponseEntity.ok(configuration)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html).
   */
  @PutMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      OBJECT_LOCK
    ]
  )
  @S3Verified(year = 2025)
  fun putObjectLockConfiguration(
    @PathVariable bucketName: String,
    @RequestBody configuration: ObjectLockConfiguration
  ): ResponseEntity<Void> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.setObjectLockConfiguration(bucketName, configuration)
    return ResponseEntity.ok().build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      LIFECYCLE,
      NOT_LIST_TYPE
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun getBucketLifecycleConfiguration(@PathVariable bucketName: String): ResponseEntity<BucketLifecycleConfiguration> {
    bucketService.verifyBucketExists(bucketName)
    val configuration = bucketService.getBucketLifecycleConfiguration(bucketName)
    return ResponseEntity.ok(configuration)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html).
   */
  @PutMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      LIFECYCLE
    ]
  )
  @S3Verified(year = 2025)
  fun putBucketLifecycleConfiguration(
    @PathVariable bucketName: String,
    @RequestBody configuration: BucketLifecycleConfiguration
  ): ResponseEntity<Void> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.setBucketLifecycleConfiguration(bucketName, configuration)
    return ResponseEntity.ok().build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html).
   */
  @DeleteMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      LIFECYCLE
    ]
  )
  @S3Verified(year = 2025)
  fun deleteBucketLifecycleConfiguration(@PathVariable bucketName: String): ResponseEntity<Void> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.deleteBucketLifecycleConfiguration(bucketName)
    return ResponseEntity.noContent().build()
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      LOCATION
    ]
  )
  @S3Verified(year = 2025)
  fun getBucketLocation(@PathVariable bucketName: String): ResponseEntity<LocationConstraint> {
    val bucketMetadata = bucketService.verifyBucketExists(bucketName)
    val bucketRegion = bucketMetadata.bucketRegion
    return ResponseEntity.ok(LocationConstraint(bucketRegion))
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html).
   *
   * @see .listObjectsV2
   *
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      NOT_UPLOADS,
      NOT_OBJECT_LOCK,
      NOT_LIST_TYPE,
      NOT_LIFECYCLE,
      NOT_LOCATION,
      NOT_VERSIONS,
      NOT_VERSIONING
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  @Deprecated("Long since replaced by listObjectsV2")
  fun listObjects(
    @PathVariable bucketName: String,
    @RequestParam(required = false) delimiter: String?,
    @RequestParam(name = ENCODING_TYPE, required = false) encodingType: String?,
    @RequestParam(required = false) marker: String?,
    @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) maxKeys: Int,
    @RequestParam(required = false) prefix: String?
  ): ResponseEntity<ListBucketResult> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.verifyMaxKeys(maxKeys)
    bucketService.verifyEncodingType(encodingType)
    val listBucketResult = bucketService.listObjectsV1(
      bucketName,
      prefix,
      delimiter,
      marker,
      encodingType,
      maxKeys
    )
    return ResponseEntity.ok(listBucketResult)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      LIST_TYPE_V2
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun listObjectsV2(
    @PathVariable bucketName: String,
    @RequestParam(name = CONTINUATION_TOKEN, required = false) continuationToken: String?,
    @RequestParam(required = false) delimiter: String?,
    @RequestParam(name = ENCODING_TYPE, required = false) encodingType: String?,
    @RequestParam(name = FETCH_OWNER, defaultValue = "false") fetchOwner: Boolean,
    @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) maxKeys: Int,
    @RequestParam(required = false) prefix: String?,
    @RequestParam(name = START_AFTER, required = false) startAfter: String?
  ): ResponseEntity<ListBucketResultV2> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.verifyMaxKeys(maxKeys)
    bucketService.verifyEncodingType(encodingType)
    val listBucketResultV2 =
      bucketService.listObjectsV2(
        bucketName,
        prefix,
        delimiter,
        encodingType,
        startAfter,
        maxKeys,
        continuationToken,
        fetchOwner
      )

    return ResponseEntity.ok(listBucketResultV2)
  }

  /**
   * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html).
   */
  @GetMapping(
    value = [
      // AWS SDK V2 pattern
      "/{bucketName:.+}",
      // AWS SDK V1 pattern
      "/{bucketName:.+}/"
    ],
    params = [
      VERSIONS
    ],
    produces = [
      MediaType.APPLICATION_XML_VALUE
    ]
  )
  @S3Verified(year = 2025)
  fun listObjectVersions(
    @PathVariable bucketName: String,
    @RequestParam(required = false) delimiter: String?,
    @RequestParam(name = ENCODING_TYPE, required = false) encodingType: String?,
    @RequestParam(name = KEY_MARKER, required = false) keyMarker: String?,
    @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) maxKeys: Int,
    @RequestParam(required = false) prefix: String?,
    @RequestParam(name = VERSION_ID_MARKER, required = false) versionIdMarker: String?
  ): ResponseEntity<ListVersionsResult> {
    bucketService.verifyBucketExists(bucketName)
    bucketService.verifyMaxKeys(maxKeys)
    bucketService.verifyEncodingType(encodingType)
    val listVersionsResult = bucketService.listVersions(
      bucketName,
      prefix,
      delimiter,
      encodingType,
      maxKeys,
      keyMarker,
      versionIdMarker
    )

    return ResponseEntity.ok(listVersionsResult)
  }

  private fun regionFrom(createBucketRequest: CreateBucketConfiguration?): String? =
    createBucketRequest?.regionFrom()
}
