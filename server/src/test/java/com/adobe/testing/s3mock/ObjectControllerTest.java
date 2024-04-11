/*
 *  Copyright 2017-2024 Adobe.
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

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_MD5;
import static com.adobe.testing.s3mock.dto.Grant.Permission.FULL_CONTROL;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ACL;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.RETENTION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;
import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.CanonicalUser;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.TagSet;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.util.UriComponentsBuilder;

@MockBeans({@MockBean(classes = {KmsKeyStore.class, MultipartService.class,
    BucketController.class, MultipartController.class})})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ObjectControllerTest extends BaseControllerTest {
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  @MockBean
  private ObjectService objectService;
  @MockBean
  private BucketService bucketService;
  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void testPutObject_Ok() throws Exception {
    givenBucket();
    var key = "sampleFile.txt";

    var testFile = new File(UPLOAD_FILE_NAME);
    var digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

    when(objectService.putS3Object(
        eq(TEST_BUCKET_NAME),
        eq(key),
        contains(TEXT_PLAIN_VALUE),
        anyMap(),
        isNull(),
        eq(false),
        anyMap(),
        anyMap(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD))
    ).thenReturn(s3ObjectMetadata(key, digest));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    var response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.PUT,
        new HttpEntity<>(FileUtils.readFileToByteArray(testFile), headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getHeaders().getETag()).isEqualTo("\"" + digest + "\"");
  }


  @Test
  void testPutObject_Options() throws Exception {
    givenBucket();
    var key = "sampleFile.txt";

    var testFile = new File(UPLOAD_FILE_NAME);
    var digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

    when(objectService.putS3Object(
        eq(TEST_BUCKET_NAME),
        eq(key),
        contains(MediaType.TEXT_PLAIN_VALUE),
        anyMap(),
        isNull(),
        eq(false),
        anyMap(),
        anyMap(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD))
    ).thenReturn(s3ObjectMetadata(key, digest));

    var optionsResponse = restTemplate.optionsForAllow("/test-bucket/" + key);

    assertThat(optionsResponse).contains(HttpMethod.PUT);

    var origin = "http://www.someurl.com";
    var putHeaders = new HttpHeaders();
    putHeaders.setAccept(List.of(APPLICATION_XML));
    putHeaders.setContentType(TEXT_PLAIN);
    putHeaders.setOrigin(origin);

    var putResponse = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.PUT,
        new HttpEntity<>(FileUtils.readFileToByteArray(testFile), putHeaders),
        String.class
    );

    assertThat(putResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(putResponse.getHeaders().getETag()).isEqualTo("\"" + digest + "\"");
  }

  @Test
  void testPutObject_md5_Ok() throws Exception {
    givenBucket();
    var key = "sampleFile.txt";

    var testFile = new File(UPLOAD_FILE_NAME);
    var hexDigest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

    when(objectService.putS3Object(
        eq(TEST_BUCKET_NAME),
        eq(key),
        contains(TEXT_PLAIN_VALUE),
        anyMap(),
        isNull(),
        eq(false),
        anyMap(),
        anyMap(),
        isNull(),
        isNull(),
        isNull(),
        eq(Owner.DEFAULT_OWNER),
        eq(StorageClass.STANDARD))
    ).thenReturn(s3ObjectMetadata(key, hexDigest));

    var base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));
    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    headers.set(CONTENT_MD5, base64Digest);

    var response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.PUT,
        new HttpEntity<>(FileUtils.readFileToByteArray(testFile), headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getHeaders().getETag()).isEqualTo("\"" + hexDigest + "\"");
  }

  @Test
  void testPutObject_md5_BadRequest() throws Exception {
    givenBucket();

    var testFile = new File(UPLOAD_FILE_NAME);
    var base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));

    doThrow(BAD_REQUEST_MD5).when(
        objectService).verifyMd5(any(InputStream.class), eq(base64Digest + 1), isNull());

    var key = "sampleFile.txt";
    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    headers.set(CONTENT_MD5, base64Digest + 1);

    var response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.PUT,
        new HttpEntity<>(FileUtils.readFileToByteArray(testFile), headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void testGetObject_Encrypted_Ok() {
    givenBucket();
    var encryption = "aws:kms";
    var encryptionKey = "key-ref";
    var key = "name";
    var expectedS3ObjectMetadata = s3ObjectEncrypted(key, "digest",
        encryption, encryptionKey);

    when(objectService.verifyObjectExists(TEST_BUCKET_NAME, key))
        .thenReturn(expectedS3ObjectMetadata);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    var response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        Void.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getHeaders().get(X_AMZ_SERVER_SIDE_ENCRYPTION))
        .containsExactly(encryption);
    assertThat(response.getHeaders().get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID))
        .containsExactly(encryptionKey);
  }

  @Test
  void testHeadObject_Encrypted_Ok() {
    givenBucket();
    var encryption = "aws:kms";
    var encryptionKey = "key-ref";
    var key = "name";
    var expectedS3ObjectMetadata = s3ObjectEncrypted(key, "digest",
        encryption, encryptionKey);

    when(objectService.verifyObjectExists("test-bucket", key))
        .thenReturn(expectedS3ObjectMetadata);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    var response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.HEAD,
        new HttpEntity<>(headers),
        Void.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getHeaders().get(X_AMZ_SERVER_SIDE_ENCRYPTION))
        .containsExactly(encryption);
    assertThat(response.getHeaders().get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID))
        .containsExactly(encryptionKey);
  }

  @Test
  void testHeadObject_NotFound() {
    givenBucket();
    var key = "name";

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    var response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.HEAD,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetObjectAcl_Ok() throws JsonProcessingException {
    givenBucket();
    var key = "name";

    var owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    var grantee = new CanonicalUser(owner.id(), owner.displayName(), null, null);
    var policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    when(objectService.getAcl("test-bucket", key)).thenReturn(policy);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(ACL, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(policy));
  }

  @Test
  void testPutObjectAcl_Ok() throws Exception {
    givenBucket();
    var key = "name";

    var owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    var grantee = new CanonicalUser(owner.id(), owner.displayName(), null, null);
    var policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(ACL, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(policy), headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    verify(objectService).setAcl("test-bucket", key, policy);
  }

  @Test
  void testGetObjectTagging_Ok() throws Exception {
    givenBucket();
    var key = "name";
    var tagging = new Tagging(new TagSet(Arrays.asList(
        new Tag("key1", "value1"), new Tag("key2", "value2"))
    ));
    var s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString(),
        null, null, null, tagging.tagSet().tags());
    when(objectService.verifyObjectExists("test-bucket", key))
        .thenReturn(s3ObjectMetadata);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(TAGGING, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(tagging));
  }

  @Test
  void testPutObjectTagging_Ok() throws Exception {
    givenBucket();
    var key = "name";
    var s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString());
    when(objectService.verifyObjectExists("test-bucket", key))
        .thenReturn(s3ObjectMetadata);
    var tagging = new Tagging(new TagSet(Arrays.asList(
        new Tag("key1", "value1"), new Tag("key2", "value2"))
    ));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(TAGGING, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(tagging), headers),
        String.class
    );

    verify(objectService).setObjectTags("test-bucket", key, tagging.tagSet().tags());
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void testGetObjectRetention_Ok() throws Exception {
    givenBucket();
    var key = "name";
    var instant = Instant.ofEpochMilli(1514477008120L);
    var retention = new Retention(Mode.COMPLIANCE, instant);
    var s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString(),
        null, null, retention, null);
    when(objectService.verifyObjectLockConfiguration("test-bucket", key))
        .thenReturn(s3ObjectMetadata);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(RETENTION, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(retention));
  }

  @Test
  void testPutObjectRetention_Ok() throws Exception {
    givenBucket();
    var key = "name";
    var instant = Instant.ofEpochMilli(1514477008120L);
    var retention = new Retention(Mode.COMPLIANCE, instant);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(RETENTION, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(retention), headers),
        String.class
    );

    verify(objectService).setRetention("test-bucket", key, retention);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  void givenBucket() {
    when(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
    when(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);
  }

  static S3ObjectMetadata s3ObjectEncrypted(
      String id, String digest, String encryption, String encryptionKey) {
    return s3ObjectMetadata(
        id, digest, encryption, encryptionKey, null, null
    );
  }

  static S3ObjectMetadata s3ObjectMetadata(String id, String digest) {
    return s3ObjectMetadata(id, digest, null, null, null, null);
  }

  static S3ObjectMetadata s3ObjectMetadata(String id, String digest,
      String encryption, String encryptionKey,
      Retention retention, List<Tag> tags) {
    return new S3ObjectMetadata(
        UUID.randomUUID(),
        id,
        "1234",
        "1234",
        digest,
        null,
        1L,
        Path.of(UPLOAD_FILE_NAME),
        null,
        tags,
        null,
        retention,
        null,
        null,
        encryptionHeaders(encryption, encryptionKey),
        null,
        null,
        StorageClass.STANDARD
    );
  }

  private static Map<String, String> encryptionHeaders(String encryption, String encryptionKey) {
    Map<String, String> headers = new HashMap<>();
    headers.put(X_AMZ_SERVER_SIDE_ENCRYPTION, encryption);
    headers.put(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey);
    return headers;
  }
}


