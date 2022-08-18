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

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.FileStore;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@CrossOrigin(origins = "*")
@RequestMapping("${com.adobe.testing.s3mock.contextPath:}")
public class BucketController extends ControllerBase {
  private static final Logger LOG = LoggerFactory.getLogger(BucketController.class);

  public BucketController(FileStore fileStore, BucketStore bucketStore) {
    super(fileStore, bucketStore);
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
    return ResponseEntity.ok(new ListAllMyBucketsResult(TEST_OWNER, bucketStore.listBuckets()));
  }

  //================================================================================================
  // /{bucketName:[a-z0-9.-]+}
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
          "/{bucketName:[a-z0-9.-]+}",
          "/{bucketName:.+}"
      },
      method = RequestMethod.PUT
  )
  public ResponseEntity<Void> createBucket(@PathVariable final String bucketName) {
    if (!bucketName.matches("[a-z0-9.-]+")) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidBucketName",
          "The specified bucket is not valid.");
    }

    verifyBucketDoesNotExist(bucketName);

    try {
      bucketStore.createBucket(bucketName);
      return ResponseEntity.ok().build();
    } catch (RuntimeException e) {
      LOG.error("Bucket could not be created!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
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
      value = "/{bucketName:[a-z0-9.-]+}",
      method = RequestMethod.HEAD
  )
  public ResponseEntity<Void> headBucket(@PathVariable final String bucketName) {
    if (bucketStore.doesBucketExist(bucketName)) {
      return ResponseEntity.ok().build();
    } else {
      return ResponseEntity.notFound().build();
    }
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
      value = "/{bucketName:[a-z0-9.-]+}",
      method = RequestMethod.DELETE
  )
  public ResponseEntity<Void> deleteBucket(@PathVariable final String bucketName) {
    verifyBucketExists(bucketName);

    final boolean deleted;

    try {
      if (!fileStore.getS3Objects(bucketName, null).isEmpty()) {
        throw new S3Exception(CONFLICT.value(), "BucketNotEmpty",
            "The bucket you tried to delete is not empty.");
      }
      deleted = bucketStore.deleteBucket(bucketName);
    } catch (final IOException e) {
      LOG.error("Bucket could not be deleted!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }

    if (deleted) {
      return ResponseEntity.noContent().build();
    } else {
      return ResponseEntity.notFound().build();
    }
  }
}
