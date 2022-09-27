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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Grantee;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.adobe.testing.s3mock.util.XmlUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@MockBeans({@MockBean(classes = {KmsKeyStore.class, BucketStore.class, MultipartService.class,
  BucketController.class, MultipartController.class})})
@SpringBootTest(classes = {S3MockConfiguration.class})
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
  private MockMvc mockMvc;

  @Test
  void testPutObject_Ok() throws Exception {
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

    mockMvc.perform(
            put("/test-bucket/" + key)
                .accept(APPLICATION_XML)
                .contentType(MediaType.TEXT_PLAIN_VALUE)
                .content(FileUtils.readFileToByteArray(testFile))
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.header().string("etag", "\"" + digest + "\""));
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
    mockMvc.perform(
            options("/test-bucket/" + key)
                .header("Access-Control-Request-Method", method)
                .header("Origin", origin)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andDo(print())
        .andExpect(header().string("Access-Control-Allow-Origin", origin))
        .andExpect(header().string("Access-Control-Allow-Methods", method));

    mockMvc.perform(
            put("/test-bucket/" + key)
                .accept(APPLICATION_XML)
                .header("Origin", origin)
                .contentType(MediaType.TEXT_PLAIN_VALUE)
                .content(FileUtils.readFileToByteArray(testFile))
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.header().string("ETag", "\"" + digest + "\""))
        .andExpect(header().string("Access-Control-Allow-Origin", "*"))
        .andExpect(header().string("Access-Control-Expose-Headers", "*"));
  }

  @Test
  void testPutObject_md5_Ok() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String hexDigest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));
    String base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));

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
    ).thenReturn(s3ObjectMetadata(key, hexDigest));

    mockMvc.perform(
            put("/test-bucket/" + key)
                .accept(APPLICATION_XML)
                .contentType(MediaType.TEXT_PLAIN_VALUE)
                .header(CONTENT_MD5, base64Digest)
                .content(FileUtils.readFileToByteArray(testFile))
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(header().string("etag", "\"" + hexDigest + "\""));
  }

  @Test
  void testPutObject_md5_BadRequest() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));

    doThrow(BAD_REQUEST_MD5).when(
        objectService).verifyMd5(any(InputStream.class), eq(base64Digest + 1), isNull());

    mockMvc.perform(
        put("/test-bucket/" + key)
            .accept(APPLICATION_XML)
            .contentType(MediaType.TEXT_PLAIN_VALUE)
            .content(FileUtils.readFileToByteArray(testFile))
            .header(CONTENT_MD5, base64Digest + 1)
    ).andExpect(MockMvcResultMatchers.status().isBadRequest());
  }

  @Test
  void testGetObject_Encrypted_Ok() throws Exception {
    String encryption = "aws:kms";
    String encryptionKey = "key-ref";
    String key = "name";
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, "digest",
        encryption, encryptionKey);

    givenBucket();
    when(objectService.verifyObjectExists(eq(TEST_BUCKET_NAME), eq(key)))
        .thenReturn(expectedS3ObjectMetadata);

    mockMvc.perform(
            get("/test-bucket/" + key)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(header().string(X_AMZ_SERVER_SIDE_ENCRYPTION, encryption))
        .andExpect(header().string(
            X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey));
  }

  @Test
  void testHeadObject_Encrypted_Ok() throws Exception {
    String encryption = "aws:kms";
    String encryptionKey = "key-ref";
    String key = "name";
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, "digest",
        encryption, encryptionKey);

    givenBucket();
    when(objectService.verifyObjectExists(eq("test-bucket"), eq(key)))
        .thenReturn(expectedS3ObjectMetadata);

    mockMvc.perform(
            head("/test-bucket/" + key)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(header().string(X_AMZ_SERVER_SIDE_ENCRYPTION, encryption))
        .andExpect(header().string(
            X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey));
  }

  @Test
  void testHeadObject_NotFound() throws Exception {
    String key = "name";

    givenBucket();

    mockMvc.perform(
        head("/test-bucket/" + key)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testGetObjectAcl_Ok() throws Exception {
    String key = "name";

    Owner owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    Grantee grantee = Grantee.from(owner);
    AccessControlPolicy policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    givenBucket();
    when(objectService.getAcl(eq("test-bucket"), eq(key)))
        .thenReturn(policy);

    mockMvc.perform(
            get("/test-bucket/" + key)
                .param(ACL, "ignored")
                .accept(APPLICATION_XML)
                .contentType(APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(XmlUtil.serializeJaxb(policy)));
  }

  @Test
  void testPutObjectAcl_Ok() throws Exception {
    String key = "name";

    Owner owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    Grantee grantee = Grantee.from(owner);
    AccessControlPolicy policy = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );

    givenBucket();

    mockMvc.perform(
            put("/test-bucket/" + key)
                .param(ACL, "ignored")
                .accept(APPLICATION_XML)
                .contentType(APPLICATION_XML)
                .content(XmlUtil.serializeJaxb(policy))
        ).andExpect(MockMvcResultMatchers.status().isOk());

    verify(objectService).setAcl(eq("test-bucket"), eq(key), eq(policy));
  }

  @Test
  void testGetObjectTagging_Ok() throws Exception {
    String key = "name";

    Tagging tagging = new Tagging(Arrays.asList(
        new Tag("key1", "value1"), new Tag("key2", "value2"))
    );
    givenBucket();
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString(),
        null, null, null, tagging.tagSet());
    when(objectService.verifyObjectExists(eq("test-bucket"), eq(key)))
        .thenReturn(s3ObjectMetadata);

    mockMvc.perform(
            get("/test-bucket/" + key)
                .param(TAGGING, "ignored")
                .accept(APPLICATION_XML)
                .contentType(APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(tagging)));
  }

  @Test
  void testPutObjectTagging_Ok() throws Exception {
    String key = "name";
    Tagging tagging = new Tagging(Arrays.asList(
        new Tag("key1", "value1"), new Tag("key2", "value2"))
    );
    givenBucket();
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString());
    when(objectService.verifyObjectExists(eq("test-bucket"), eq(key)))
        .thenReturn(s3ObjectMetadata);
    mockMvc.perform(
            put("/test-bucket/" + key)
                .param(TAGGING, "ignored")
                .accept(APPLICATION_XML)
                .contentType(APPLICATION_XML)
                .content(MAPPER.writeValueAsString(tagging))
        ).andExpect(MockMvcResultMatchers.status().isOk());

    verify(objectService).setObjectTags(eq("test-bucket"), eq(key), eq(tagging.tagSet()));
  }

  @Test
  void testGetObjectRetention_Ok() throws Exception {
    String key = "name";
    Instant instant = Instant.ofEpochMilli(1514477008120L);
    Retention retention = new Retention(Mode.COMPLIANCE, instant);
    givenBucket();
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(key, UUID.randomUUID().toString(),
        null, null, retention, null);
    when(objectService.verifyObjectLockConfiguration(eq("test-bucket"), eq(key)))
        .thenReturn(s3ObjectMetadata);

    mockMvc.perform(
            get("/test-bucket/" + key)
                .param(RETENTION, "ignored")
                .accept(APPLICATION_XML)
                .contentType(APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(retention)));
  }

  @Test
  void testPutObjectRetention_Ok() throws Exception {
    String key = "name";
    Instant instant = Instant.ofEpochMilli(1514477008120L);
    Retention retention = new Retention(Mode.COMPLIANCE, instant);
    givenBucket();
    mockMvc.perform(
            put("/test-bucket/" + key)
                .param(RETENTION, "ignored")
                .accept(APPLICATION_XML)
                .contentType(APPLICATION_XML)
                .content(MAPPER.writeValueAsString(retention))
        ).andExpect(MockMvcResultMatchers.status().isOk());

    verify(objectService).setRetention(eq("test-bucket"), eq(key), eq(retention));
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


