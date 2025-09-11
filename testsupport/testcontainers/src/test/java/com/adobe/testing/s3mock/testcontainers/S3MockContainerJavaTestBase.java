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

package com.adobe.testing.s3mock.testcontainers;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * This class contains test and utility methods used for manual and JUnit 5 test cases.
 */
abstract class S3MockContainerJavaTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(S3MockContainerJavaTestBase.class);

  // we set the system property when running in maven, use "latest" for unit tests in the IDE
  protected static final String S3MOCK_VERSION = System.getProperty("s3mock.version", "latest");
  protected static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket-a", "bucket-b");
  protected static final String TEST_ENC_KEYREF =
      "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-ref";
  protected static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  protected S3Client s3Client;

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void testPutAndGetObject(TestInfo testInfo) throws Exception {
    var bucketName = bucketName(testInfo);
    var uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(uploadFile.getName()).build(),
        RequestBody.fromFile(uploadFile));

    var response =
        s3Client.getObject(
            GetObjectRequest.builder().bucket(bucketName).key(uploadFile.getName()).build());

    var uploadFileIs = Files.newInputStream(uploadFile.toPath());
    var uploadDigest = DigestUtil.hexDigest(uploadFileIs);
    var downloadedDigest = DigestUtil.hexDigest(response);
    uploadFileIs.close();
    response.close();

    assertThat(uploadDigest).as("Up- and downloaded Files should have equal digests")
        .isEqualTo(downloadedDigest);
  }

  /**
   * Creates a bucket, stores a file, lists the bucket.
   */
  @Test
  void testPutObjectAndListBucket(TestInfo testInfo) {
    var bucketName = bucketName(testInfo);
    var uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(uploadFile.getName()).build(),
        RequestBody.fromFile(uploadFile));

    var listObjectsV2Response =
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());

    assertThat(listObjectsV2Response.contents()).hasSize(1);
  }

  /**
   * Verifies that default Buckets got created after S3 Mock was bootstrapped.
   */
  @Test
  void defaultBucketsGotCreated() {
    var buckets = s3Client.listBuckets().buckets();
    var bucketNames = buckets.stream().map(Bucket::name)
        .filter(INITIAL_BUCKET_NAMES::contains).collect(Collectors.toSet());

    assertThat(bucketNames).as("Not all default Buckets got created")
        .containsAll(INITIAL_BUCKET_NAMES);
  }

  protected S3Client createS3ClientV2(String endpoint) {
    return S3Client.builder()
        .region(Region.of("us-east-1"))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .endpointOverride(URI.create(endpoint))
        .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(
            AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .build();
  }

  protected String bucketName(TestInfo testInfo) {
    var methodName = testInfo.getTestMethod().get().getName();
    var normalizedName = methodName.toLowerCase().replace('_', '-');
    if (normalizedName.length() > 50) {
      // max bucket name length is 63, shorten name to 50 since we add the timestamp below.
      normalizedName = normalizedName.substring(0, 50);
    }
    var timestamp = Instant.now().getEpochSecond();
    return String.format("%s-%d", normalizedName, timestamp);
  }
}
