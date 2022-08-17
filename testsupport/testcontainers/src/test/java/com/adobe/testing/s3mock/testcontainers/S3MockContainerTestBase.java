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

package com.adobe.testing.s3mock.testcontainers;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * This class contains test and utility methods used for manual and JUnit 5 test cases.
 */
abstract class S3MockContainerTestBase {

  // we set the system property when running in maven, use "latest" for unit tests in the IDE
  protected static final String S3MOCK_VERSION = System.getProperty("s3mock.version", "latest");
  protected static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket-a", "bucket-b");
  protected static final String TEST_ENC_KEYREF =
      "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-ref";
  protected static final String BUCKET_NAME = "mydemotestbucket";
  protected static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  protected S3Client s3Client;

  /**
   * Creates a bucket, stores a file, downloads the file again and compares checksums.
   *
   * @throws Exception if FileStreams can not be read
   */
  @Test
  void shouldUploadAndDownloadObject() throws Exception {
    final File uploadFile = new File(UPLOAD_FILE_NAME);

    s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
    s3Client.putObject(
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(uploadFile.getName()).build(),
        RequestBody.fromFile(uploadFile));

    final ResponseInputStream<GetObjectResponse> response =
        s3Client.getObject(
            GetObjectRequest.builder().bucket(BUCKET_NAME).key(uploadFile.getName()).build());

    final InputStream uploadFileIs = Files.newInputStream(uploadFile.toPath());
    final String uploadDigest = DigestUtil.getHexDigest(uploadFileIs);
    final String downloadedDigest = DigestUtil.getHexDigest(response);
    uploadFileIs.close();
    response.close();

    assertThat(uploadDigest).isEqualTo(downloadedDigest).as(
        "Up- and downloaded Files should have equal digests");
  }

  /**
   * Verifies that default Buckets got created after S3 Mock was bootstrapped.
   */
  @Test
  void defaultBucketsGotCreated() {
    final List<Bucket> buckets = s3Client.listBuckets().buckets();
    final Set<String> bucketNames = buckets.stream().map(Bucket::name)
        .filter(INITIAL_BUCKET_NAMES::contains).collect(Collectors.toSet());

    assertThat(bucketNames).as("Not all default Buckets got created")
        .containsAll(INITIAL_BUCKET_NAMES);
  }

  protected S3Client createS3ClientV2(String endpoint) {
    return S3Client.builder()
        .region(Region.of("us-east-1"))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .endpointOverride(URI.create(endpoint))
        .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(
            AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .build();
  }
}
