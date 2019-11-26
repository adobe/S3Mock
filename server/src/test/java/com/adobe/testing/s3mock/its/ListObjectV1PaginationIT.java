/*
 *  Copyright 2017-2019 Adobe.
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

package com.adobe.testing.s3mock.its;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListObjectV1PaginationIT extends S3TestBase {

  private final String bucketName = "some-bucket";

  @BeforeEach
  @Override
  public void prepareS3Client() {
    super.prepareS3Client();
    s3Client.createBucket(bucketName);
    s3Client.putObject(bucketName, "a", "");
    s3Client.putObject(bucketName, "b", "");
  }

  @Test
  public void shouldTruncateAndReturnNextMarker() {
    final ListObjectsRequest request =
        new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(1);
    final ObjectListing objectListing = s3Client.listObjects(request);

    assertEquals(1, objectListing.getObjectSummaries().size());
    assertEquals(1, objectListing.getMaxKeys());
    assertEquals("a", objectListing.getNextMarker());
    assertTrue(objectListing.isTruncated());

    final ListObjectsRequest continueRequest = new ListObjectsRequest().withBucketName(bucketName)
        .withMarker(objectListing.getNextMarker());
    final ObjectListing continueObjectListing = s3Client.listObjects(continueRequest);
    assertEquals(1, continueObjectListing.getObjectSummaries().size());
    assertEquals("b", continueObjectListing.getObjectSummaries().get(0).getKey());
  }
}
