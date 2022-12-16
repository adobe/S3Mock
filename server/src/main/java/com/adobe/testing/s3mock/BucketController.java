/*
 *  Copyright 2017-2022 Adobe.
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

package com.adobe.testing.s3mock;

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_BUCKET_OBJECT_LOCK_ENABLED;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.CONTINUATION_TOKEN;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LIST_TYPE_V2;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIST_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_OBJECT_LOCK;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.OBJECT_LOCK;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.START_AFTER;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.service.BucketService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Handles requests related to buckets.
 */
@CrossOrigin(origins = "*")
@RequestMapping("${com.adobe.testing.s3mock.contextPath:}")
public class BucketController {
  private final BucketService bucketService;

  public BucketController(BucketService bucketService) {
    this.bucketService = bucketService;
  }

  //================================================================================================
  // /
  //================================================================================================

  /**
   * List all existing buckets.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html">API Reference</a>
   *
   * @return List of all Buckets
   */
  @RequestMapping(
      value = "/",
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListAllMyBucketsResult> listBuckets() {
    ListAllMyBucketsResult listAllMyBucketsResult = bucketService.listBuckets();
    return ResponseEntity.ok(listAllMyBucketsResult);
  }

  //================================================================================================
  // /{bucketName:.+}
  //================================================================================================

  /**
   * Create a bucket if the name matches a simplified version of the bucket naming rules.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">API Reference Bucket Naming</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html">API Reference</a>
   *
   * @param bucketName name of the bucket that should be created.
   *
   * @return 200 OK if creation was successful.
   */
  @RequestMapping(
      value = {
          "/{bucketName:.+}"
      },
      params = {
          NOT_OBJECT_LOCK,
          NOT_LIFECYCLE
      },
      method = RequestMethod.PUT
  )
  public ResponseEntity<Void> createBucket(@PathVariable final String bucketName,
      @RequestHeader(value = X_AMZ_BUCKET_OBJECT_LOCK_ENABLED,
          required = false, defaultValue = "false") boolean objectLockEnabled) {
    bucketService.verifyBucketNameIsAllowed(bucketName);
    bucketService.verifyBucketDoesNotExist(bucketName);
    bucketService.createBucket(bucketName, objectLockEnabled);
    return ResponseEntity.ok().build();
  }

  /**
   * Check if a bucket exists.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200 if it exists; 404 if not found.
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      method = RequestMethod.HEAD
  )
  public ResponseEntity<Void> headBucket(@PathVariable final String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    return ResponseEntity.ok().build();
  }

  /**
   * Delete a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 204 if Bucket was deleted; 404 if not found
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      params = {
          NOT_LIFECYCLE
      },
      method = RequestMethod.DELETE
  )
  public ResponseEntity<Void> deleteBucket(@PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketIsEmpty(bucketName);
    bucketService.deleteBucket(bucketName);
    return ResponseEntity.noContent().build();
  }

  /**
   * Get ObjectLockConfiguration of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200, ObjectLockConfiguration
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      params = {
          OBJECT_LOCK,
          NOT_LIST_TYPE
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ObjectLockConfiguration> getObjectLockConfiguration(
      @PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    ObjectLockConfiguration configuration = bucketService.getObjectLockConfiguration(bucketName);
    return ResponseEntity.ok(configuration);
  }

  /**
   * Put ObjectLockConfiguration of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200, ObjectLockConfiguration
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      params = {
          OBJECT_LOCK
      },
      method = RequestMethod.PUT,
      consumes = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Void> putObjectLockConfiguration(
      @PathVariable String bucketName,
      @RequestBody ObjectLockConfiguration configuration) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.setObjectLockConfiguration(bucketName, configuration);
    return ResponseEntity.ok().build();
  }

  /**
   * Get BucketLifecycleConfiguration of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200, ObjectLockConfiguration
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      params = {
          LIFECYCLE,
          NOT_LIST_TYPE
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<BucketLifecycleConfiguration> getBucketLifecycleConfiguration(
      @PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    BucketLifecycleConfiguration configuration =
        bucketService.getBucketLifecycleConfiguration(bucketName);
    return ResponseEntity.ok(configuration);
  }

  /**
   * Put BucketLifecycleConfiguration of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200, ObjectLockConfiguration
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      params = {
          LIFECYCLE
      },
      method = RequestMethod.PUT,
      consumes = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Void> putBucketLifecycleConfiguration(
      @PathVariable String bucketName,
      @RequestBody BucketLifecycleConfiguration configuration) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.setBucketLifecycleConfiguration(bucketName, configuration);
    return ResponseEntity.ok().build();
  }

  /**
   * Delete BucketLifecycleConfiguration of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html">API Reference</a>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200, ObjectLockConfiguration
   */
  @RequestMapping(
      value = "/{bucketName:.+}",
      params = {
          LIFECYCLE
      },
      method = RequestMethod.DELETE
  )
  public ResponseEntity<Void> deleteBucketLifecycleConfiguration(
      @PathVariable String bucketName) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.deleteBucketLifecycleConfiguration(bucketName);
    return ResponseEntity.noContent().build();
  }

  /**
   * Retrieve list of objects of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">API Reference</a>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they start with prefix
   * @param encodingType whether to use URL encoding (encodingtype="url") or not
   *
   * @return {@link ListBucketResult} a list of objects in Bucket
   * @deprecated Long since replaced by ListObjectsV2, {@see #listObjectsInsideBucketV2}
   */
  @RequestMapping(
      params = {
          NOT_UPLOADS,
          NOT_OBJECT_LOCK,
          NOT_LIST_TYPE,
          NOT_LIFECYCLE
      },
      value = "/{bucketName:.+}",
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  @Deprecated
  public ResponseEntity<ListBucketResult> listObjects(
      @PathVariable String bucketName,
      @RequestParam(required = false) String prefix,
      @RequestParam(required = false) String delimiter,
      @RequestParam(required = false) String marker,
      @RequestParam(name = ENCODING_TYPE, required = false) String encodingType,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) Integer maxKeys) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyMaxKeys(maxKeys);
    bucketService.verifyEncodingType(encodingType);
    ListBucketResult listBucketResult = bucketService.listObjectsV1(bucketName, prefix, delimiter,
        marker, encodingType, maxKeys);
    return ResponseEntity.ok(listBucketResult);
  }

  /**
   * Retrieve list of objects of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they start with prefix
   * @param startAfter {@link String} return key names after a specific object key in your key
   *     space
   * @param maxKeys {@link Integer} set maximum number of keys to be returned
   * @param continuationToken {@link String} pagination token returned by previous request
   *
   * @return {@link ListBucketResultV2} a list of objects in Bucket
   */
  @RequestMapping(value = "/{bucketName:.+}",
      params = {
          LIST_TYPE_V2
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListBucketResultV2> listObjectsV2(
      @PathVariable String bucketName,
      @RequestParam(required = false) String prefix,
      @RequestParam(required = false) String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) String encodingType,
      @RequestParam(name = START_AFTER, required = false) String startAfter,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000", required = false) Integer maxKeys,
      @RequestParam(name = CONTINUATION_TOKEN, required = false) String continuationToken) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyMaxKeys(maxKeys);
    bucketService.verifyEncodingType(encodingType);
    ListBucketResultV2 listBucketResultV2 =
        bucketService.listObjectsV2(bucketName, prefix, delimiter, encodingType, startAfter,
            maxKeys, continuationToken);

    return ResponseEntity.ok(listBucketResultV2);
  }
}
