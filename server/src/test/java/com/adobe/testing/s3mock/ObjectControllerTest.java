/*
 *  Copyright 2017-2023 Adobe.
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
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Grantee;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.TagSet;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.adobe.testing.s3mock.util.XmlUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import jakarta.xml.bind.JAXBException;
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
import java.util.Set;
import java.util.UUID;
import javax.xml.stream.XMLStreamException;
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
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;

@MockBeans({@MockBean(classes = {KmsKeyStore.class, MultipartService.class,
    BucketController.class, MultipartController.class})})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ObjectControllerTest {
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";
  private static final ObjectMapper MAPPER = new XmlMapper();

  @MockBean
  private ObjectService objectService;
  @MockBean
  private BucketService bucketService;
  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void testPutObject_Ok() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

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
        eq(Owner.DEFAULT_OWNER))
    ).thenReturn(s3ObjectMetadata(key, digest));

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    ResponseEntity<String> response = restTemplate.exchange("/test-bucket/" + key,
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
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

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
        eq(Owner.DEFAULT_OWNER))
    ).thenReturn(s3ObjectMetadata(key, digest));

    String origin = "http://www.someurl.com";
    String method = "PUT";
    HttpHeaders optionsHeaders = new HttpHeaders();
    optionsHeaders.set("Access-Control-Request-Method", method);
    optionsHeaders.setOrigin(origin);
    Set<HttpMethod> optionsResponse = restTemplate.optionsForAllow("/test-bucket/" + key);

    assertThat(optionsResponse).contains(HttpMethod.PUT);

    HttpHeaders putHeaders = new HttpHeaders();
    putHeaders.setAccept(List.of(APPLICATION_XML));
    putHeaders.setContentType(TEXT_PLAIN);
    putHeaders.setOrigin(origin);

    ResponseEntity<String> putResponse = restTemplate.exchange("/test-bucket/" + key,
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
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String hexDigest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

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
        eq(Owner.DEFAULT_OWNER))
    ).thenReturn(s3ObjectMetadata(key, hexDigest));

    String base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    headers.set(CONTENT_MD5, base64Digest);

    ResponseEntity<String> response = restTemplate.exchange("/test-bucket/" + key,
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

    File testFile = new File(UPLOAD_FILE_NAME);
    String base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));

    doThrow(BAD_REQUEST_MD5).when(
        objectService).verifyMd5(any(InputStream.class), eq(base64Digest + 1), isNull());

    String key = "sampleFile.txt";
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    headers.set(CONTENT_MD5, base64Digest + 1);

    ResponseEntity<String> response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.PUT,
        new HttpEntity<>(FileUtils.readFileToByteArray(testFile), headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void testGetObject_Encrypted_Ok() {
    givenBucket();
    String encryption = "aws:kms";
    String encryptionKey = "key-ref";
    String key = "name";
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, "digest",
        encryption, encryptionKey);

    when(objectService.verifyObjectExists(eq(TEST_BUCKET_NAME), eq(key)))
        .thenReturn(expectedS3ObjectMetadata);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    ResponseEntity<String> response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
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
    String encryption = "aws:kms";
    String encryptionKey = "key-ref";
    String key = "name";
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, "digest",
        encryption, encryptionKey);

    when(objectService.verifyObjectExists(eq("test-bucket"), eq(key)))
        .thenReturn(expectedS3ObjectMetadata);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    ResponseEntity<String> response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.HEAD,
        new HttpEntity<>(headers),
        String.class
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
    String key = "name";

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(TEXT_PLAIN);
    ResponseEntity<String> response = restTemplate.exchange("/test-bucket/" + key,
        HttpMethod.HEAD,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetObjectAcl_Ok() throws JAXBException, XMLStreamException {
    givenBucket();
    String key = "name";

    Owner owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    Grantee grantee = Grantee.from(owner);
    AccessControlPolicy policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    when(objectService.getAcl(eq("test-bucket"), eq(key)))
        .thenReturn(policy);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    String uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(ACL, "ignored").build().toString();
    ResponseEntity<String> response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(XmlUtil.deserializeJaxb(response.getBody())).isEqualTo(policy);
  }

  @Test
  void testPutObjectAcl_Ok() throws Exception {
    givenBucket();
    String key = "name";

    Owner owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    Grantee grantee = Grantee.from(owner);
    AccessControlPolicy policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    String uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(ACL, "ignored").build().toString();
    ResponseEntity<String> response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(XmlUtil.serializeJaxb(policy), headers),
        String.class
    );

    verify(objectService).setAcl(eq("test-bucket"), eq(key), eq(policy));
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void testGetObjectTagging_Ok() throws Exception {
    givenBucket();
    String key = "name";
    Tagging tagging = new Tagging(new TagSet(Arrays.asList(
        new Tag("key1", "value1"), new Tag("key2", "value2"))
    ));
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString(),
        null, null, null, tagging.tagSet().tags());
    when(objectService.verifyObjectExists(eq("test-bucket"), eq(key)))
        .thenReturn(s3ObjectMetadata);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    String uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(TAGGING, "ignored").build().toString();
    ResponseEntity<String> response = restTemplate.exchange(
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
    String key = "name";
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString());
    when(objectService.verifyObjectExists(eq("test-bucket"), eq(key)))
        .thenReturn(s3ObjectMetadata);
    Tagging tagging = new Tagging(new TagSet(Arrays.asList(
        new Tag("key1", "value1"), new Tag("key2", "value2"))
    ));

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    String uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(TAGGING, "ignored").build().toString();
    ResponseEntity<String> response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(tagging), headers),
        String.class
    );

    verify(objectService).setObjectTags(eq("test-bucket"), eq(key), eq(tagging.tagSet().tags()));
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void testGetObjectRetention_Ok() throws Exception {
    givenBucket();
    String key = "name";
    Instant instant = Instant.ofEpochMilli(1514477008120L);
    Retention retention = new Retention(Mode.COMPLIANCE, instant);
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString(),
        null, null, retention, null);
    when(objectService.verifyObjectLockConfiguration(eq("test-bucket"), eq(key)))
        .thenReturn(s3ObjectMetadata);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    String uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(RETENTION, "ignored").build().toString();
    ResponseEntity<String> response = restTemplate.exchange(
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
    String key = "name";
    Instant instant = Instant.ofEpochMilli(1514477008120L);
    Retention retention = new Retention(Mode.COMPLIANCE, instant);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    String uri = UriComponentsBuilder.fromUriString("/test-bucket/" + key)
        .queryParam(RETENTION, "ignored").build().toString();
    ResponseEntity<String> response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(retention), headers),
        String.class
    );

    verify(objectService).setRetention(eq("test-bucket"), eq(key), eq(retention));
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
        encryptionHeaders(encryption, encryptionKey)
    );
  }

  private static Map<String, String> encryptionHeaders(String encryption, String encryptionKey) {
    Map<String, String> headers = new HashMap<>();
    headers.put(X_AMZ_SERVER_SIDE_ENCRYPTION, encryption);
    headers.put(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey);
    return headers;
  }
}


