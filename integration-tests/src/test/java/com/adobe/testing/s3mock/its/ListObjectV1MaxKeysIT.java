/*
 *  Copyright 2017-2021 Adobe.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListObjectV1MaxKeysIT extends S3TestBase {

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
  public void returnsLimitedAmountOfObjectsBasedOnMaxKeys() {
    final ListObjectsRequest request =
        new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(1);
    final ObjectListing objectListing = s3Client.listObjects(request);

    assertThat(objectListing.getObjectSummaries()).hasSize(1);
    assertThat(objectListing.getMaxKeys()).isEqualTo(1);
  }

  @Test
  public void returnsAllObjectsIfMaxKeysIsDefault() {
    final ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName);
    final ObjectListing objectListing = s3Client.listObjects(request);

    assertThat(objectListing.getObjectSummaries()).hasSize(2);
    assertThat(objectListing.getMaxKeys()).isEqualTo(1000);
  }

  @Test
  public void returnsAllObjectsIfMaxKeysEqualToAmountOfObjects() {
    final ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName)
        .withMaxKeys(2);
    final ObjectListing objectListing = s3Client.listObjects(request);

    assertThat(objectListing.getObjectSummaries()).hasSize(2);
    assertThat(objectListing.getMaxKeys()).isEqualTo(2);
  }

  @Test
  public void returnsAllObjectsIfMaxKeysMoreThanAmountOfObjects() {
    final ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName)
        .withMaxKeys(3);
    final ObjectListing objectListing = s3Client.listObjects(request);

    assertThat(objectListing.getObjectSummaries()).hasSize(2);
    assertThat(objectListing.getMaxKeys()).isEqualTo(3);
  }

  @Test
  public void returnsEmptyListIfMaxKeysIsZero() {
    final ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName)
        .withMaxKeys(0);
    final ObjectListing objectListing = s3Client.listObjects(request);

    assertThat(objectListing.getObjectSummaries()).hasSize(0);
    assertThat(objectListing.getMaxKeys()).isEqualTo(0);
  }

  @Test
  public void returnsAllObjectsIfMaxKeysIsNegative() {
    final ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName)
        .withMaxKeys(-1);
    final ObjectListing objectListing = s3Client.listObjects(request);

    // Apparently, the Amazon SDK rejects negative max keys, and by default it's 1000
    assertThat(objectListing.getObjectSummaries()).hasSize(2);
    assertThat(objectListing.getMaxKeys()).isEqualTo(1000);
  }
}
