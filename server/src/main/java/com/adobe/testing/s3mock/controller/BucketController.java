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

package com.adobe.testing.s3mock.controller;

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_OBJECT_LOCK_ENABLED;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_REGION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_OWNERSHIP;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.BUCKET_REGION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.CONTINUATION_TOKEN;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.FETCH_OWNER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.KEY_MARKER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LIST_TYPE_V2;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LOCATION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_BUCKETS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIST_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LOCATION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_OBJECT_LOCK;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_VERSIONING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_VERSIONS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.OBJECT_LOCK;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.START_AFTER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.VERSIONING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.VERSIONS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID_MARKER;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.S3Verified;
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.CreateBucketConfiguration;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListVersionsResult;
import com.adobe.testing.s3mock.dto.LocationConstraint;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.dto.ObjectOwnership;
import com.adobe.testing.s3mock.dto.Region;
import com.adobe.testing.s3mock.dto.VersioningConfiguration;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.store.BucketMetadata;
import org.jspecify.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@CrossOrigin(origins = "*", exposedHeaders = "*")
@Controller
@RequestMapping("${com.adobe.testing.s3mock.controller.contextPath:}")
public class BucketController {
  private final BucketService bucketService;

  public BucketController(BucketService bucketService) {
    this.bucketService = bucketService;
  }

  // ===============================================================================================
  // /
  // ===============================================================================================

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html">API Reference</a>.
   */
  @GetMapping(
      value = "/",
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<ListAllMyBucketsResult> listBuckets(
      @RequestParam(name = BUCKET_REGION, required = false) Region bucketRegion,
      @RequestParam(name = CONTINUATION_TOKEN, required = false) String continuationToken,
      @RequestParam(name = MAX_BUCKETS, defaultValue = "1000", required = false) Integer maxBuckets,
      @RequestParam(required = false) String prefix
  ) {
    var listAllMyBucketsResult = bucketService.listBuckets(
        bucketRegion,
        continuationToken,
        maxBuckets,
        prefix
    );
    return ResponseEntity.ok(listAllMyBucketsResult);
  }

  // ===============================================================================================
  // /{bucketName:.+}
  // ===============================================================================================

  /**
   * Create a bucket if the name matches a simplified version of the bucket naming rules.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">API Reference Bucket Naming</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html">API Reference</a>
   */
  @PutMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          NOT_OBJECT_LOCK,
          NOT_LIFECYCLE,
          NOT_VERSIONING
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> createBucket(
      @PathVariable final String bucketName,
      @RequestHeader(value = X_AMZ_BUCKET_OBJECT_LOCK_ENABLED, required = false,
          defaultValue = "false") boolean objectLockEnabled,
      @RequestHeader(value = X_AMZ_OBJECT_OWNERSHIP, required = false,
          defaultValue = "BucketOwnerEnforced") ObjectOwnership objectOwnership,
      @RequestBody(required = false) @Nullable CreateBucketConfiguration createBucketRequest) {
    bucketService.verifyBucketNameIsAllowed(bucketName);
    bucketService.verifyBucketDoesNotExist(bucketName);
    bucketService.createBucket(bucketName,
        objectLockEnabled,
        objectOwnership,
        regionFrom(createBucketRequest),
        createBucketRequest != null ? createBucketRequest.bucket() : null,
        createBucketRequest != null ? createBucketRequest.location() : null
    );
    return ResponseEntity.ok()
        .header(LOCATION, "/" + bucketName)
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html">API Reference</a>.
   */
  @RequestMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      method = RequestMethod.HEAD
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> headBucket(@PathVariable final String bucketName) {
    BucketMetadata bucketMetadata = bucketService.verifyBucketExists(bucketName);
    return ResponseEntity
        .ok()
        .header(X_AMZ_BUCKET_REGION, bucketMetadata.bucketRegion())
        .headers(h -> h.setAll(bucketService.bucketLocationHeaders(bucketMetadata)))
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html">API Reference</a>.
   */
  @DeleteMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          NOT_LIFECYCLE
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> deleteBucket(@PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketIsEmpty(bucketName);
    bucketService.deleteBucket(bucketName);
    return ResponseEntity.noContent().build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">API Reference</a>.
   */
  @GetMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          VERSIONING,
          NOT_LIST_TYPE
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<VersioningConfiguration> getVersioningConfiguration(@PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    var configuration = bucketService.getVersioningConfiguration(bucketName);
    return ResponseEntity.ok(configuration);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">API Reference</a>.
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
      },
      consumes = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putVersioningConfiguration(
      @PathVariable String bucketName,
      @RequestBody VersioningConfiguration configuration) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.setVersioningConfiguration(bucketName, configuration);
    return ResponseEntity.ok().build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html">API Reference</a>.
   */
  @GetMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          OBJECT_LOCK,
          NOT_LIST_TYPE
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<ObjectLockConfiguration> getObjectLockConfiguration(@PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    var configuration = bucketService.getObjectLockConfiguration(bucketName);
    return ResponseEntity.ok(configuration);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html">API Reference</a>.
   */
  @PutMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          OBJECT_LOCK
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putObjectLockConfiguration(
      @PathVariable String bucketName,
      @RequestBody ObjectLockConfiguration configuration) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.setObjectLockConfiguration(bucketName, configuration);
    return ResponseEntity.ok().build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html">API Reference</a>.
   */
  @GetMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          LIFECYCLE,
          NOT_LIST_TYPE
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<BucketLifecycleConfiguration> getBucketLifecycleConfiguration(@PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    var configuration = bucketService.getBucketLifecycleConfiguration(bucketName);
    return ResponseEntity.ok(configuration);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">API Reference</a>.
   */
  @PutMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          LIFECYCLE
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putBucketLifecycleConfiguration(
      @PathVariable String bucketName,
      @RequestBody BucketLifecycleConfiguration configuration) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.setBucketLifecycleConfiguration(bucketName, configuration);
    return ResponseEntity.ok().build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html">API Reference</a>.
   */
  @DeleteMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          LIFECYCLE
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> deleteBucketLifecycleConfiguration(@PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.deleteBucketLifecycleConfiguration(bucketName);
    return ResponseEntity.noContent().build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:.+}",
      params = {
          LOCATION
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<LocationConstraint> getBucketLocation(@PathVariable String bucketName) {
    BucketMetadata bucketMetadata = bucketService.verifyBucketExists(bucketName);
    String bucketRegion = bucketMetadata.bucketRegion();
    return ResponseEntity.ok(new LocationConstraint(bucketRegion));
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">API Reference</a>.
   *
   * @see #listObjectsV2
   * @deprecated Long since replaced by listObjectsV2
   */
  @GetMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          NOT_UPLOADS,
          NOT_OBJECT_LOCK,
          NOT_LIST_TYPE,
          NOT_LIFECYCLE,
          NOT_LOCATION,
          NOT_VERSIONS,
          NOT_VERSIONING
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  @Deprecated(since = "2.12.2", forRemoval = true)
  public ResponseEntity<ListBucketResult> listObjects(
      @PathVariable String bucketName,
      @RequestParam(required = false) String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) String encodingType,
      @RequestParam(required = false) String marker,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) Integer maxKeys,
      @RequestParam(required = false) String prefix
  ) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyMaxKeys(maxKeys);
    bucketService.verifyEncodingType(encodingType);
    var listBucketResult = bucketService.listObjectsV1(bucketName, prefix, delimiter,
        marker, encodingType, maxKeys);
    return ResponseEntity.ok(listBucketResult);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>.
   */
  @GetMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          LIST_TYPE_V2
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<ListBucketResultV2> listObjectsV2(
      @PathVariable String bucketName,
      @RequestParam(name = CONTINUATION_TOKEN, required = false) String continuationToken,
      @RequestParam(required = false) String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) String encodingType,
      @RequestParam(name = FETCH_OWNER, defaultValue = "false") boolean fetchOwner,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) Integer maxKeys,
      @RequestParam(required = false) String prefix,
      @RequestParam(name = START_AFTER, required = false) String startAfter
  ) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyMaxKeys(maxKeys);
    bucketService.verifyEncodingType(encodingType);
    var listBucketResultV2 =
        bucketService.listObjectsV2(bucketName, prefix, delimiter, encodingType, startAfter,
            maxKeys, continuationToken, fetchOwner);

    return ResponseEntity.ok(listBucketResultV2);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html">API Reference</a>.
   */
  @GetMapping(
      value = {
          // AWS SDK V2 pattern
          "/{bucketName:.+}",
          // AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          VERSIONS
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<ListVersionsResult> listObjectVersions(
      @PathVariable String bucketName,
      @RequestParam(required = false) String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) String encodingType,
      @RequestParam(name = KEY_MARKER, required = false) String keyMarker,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) Integer maxKeys,
      @RequestParam(required = false) String prefix,
      @RequestParam(name = VERSION_ID_MARKER, required = false) String versionIdMarker
  ) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyMaxKeys(maxKeys);
    bucketService.verifyEncodingType(encodingType);
    var listVersionsResult = bucketService.listVersions(
        bucketName,
        prefix,
        delimiter,
        encodingType,
        maxKeys,
        keyMarker,
        versionIdMarker
    );

    return ResponseEntity.ok(listVersionsResult);
  }

  @Nullable
  private String regionFrom(@Nullable CreateBucketConfiguration createBucketRequest) {
    if (createBucketRequest != null) {
      return createBucketRequest.regionFrom();
    } else {
      return null;
    }
  }
}
