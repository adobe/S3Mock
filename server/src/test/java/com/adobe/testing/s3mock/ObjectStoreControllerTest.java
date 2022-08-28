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

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_MD5;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.DigestUtil;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
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
@MockBeans({@MockBean(classes = {KmsKeyStore.class, BucketStore.class, MultipartService.class})})
@SpringBootTest(classes = {S3MockConfiguration.class})
class ObjectStoreControllerTest {
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

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

    when(objectService.putS3Object(eq(TEST_BUCKET_NAME), eq(key),
        contains(MediaType.TEXT_PLAIN_VALUE), isNull(),
        isNull(), eq(false), anyMap(), isNull(), isNull(), isNull()))
        .thenReturn(s3ObjectMetadata(key, digest));

    mockMvc.perform(
            put("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.TEXT_PLAIN_VALUE)
                .content(FileUtils.readFileToByteArray(testFile))
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.header().string("etag", "\"" + digest + "\""));
  }

  @Test
  void testPutObject_md5_Ok() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String hexDigest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));
    String base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));

    when(objectService.putS3Object(eq(TEST_BUCKET_NAME), eq(key),
        contains(MediaType.TEXT_PLAIN_VALUE), isNull(),
        isNull(), eq(false), anyMap(), isNull(), isNull(), isNull()))
        .thenReturn(s3ObjectMetadata(key, hexDigest));

    mockMvc.perform(
            put("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.TEXT_PLAIN_VALUE)
                .header(CONTENT_MD5, base64Digest)
                .content(FileUtils.readFileToByteArray(testFile))
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.header().string("etag", "\"" + hexDigest + "\""));
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
            .accept(MediaType.APPLICATION_XML)
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
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, encryption, encryptionKey);

    givenBucket();
    when(objectService.verifyObjectExists(eq(TEST_BUCKET_NAME), eq(key)))
        .thenReturn(expectedS3ObjectMetadata);

    mockMvc.perform(
            get("/test-bucket/" + key)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.header().string(X_AMZ_SERVER_SIDE_ENCRYPTION, encryption))
        .andExpect(MockMvcResultMatchers.header().string(
            X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, encryptionKey));
  }

  @Test
  void testHeadObject_Encrypted_Ok() throws Exception {
    String encryption = "aws:kms";
    String encryptionKey = "key-ref";
    String key = "name";
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, encryption, encryptionKey);

    givenBucket();
    when(objectService.getS3Object(eq("test-bucket"), eq(key)))
        .thenReturn(expectedS3ObjectMetadata);

    mockMvc.perform(
            head("/test-bucket/" + key)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.header().string(X_AMZ_SERVER_SIDE_ENCRYPTION, encryption))
        .andExpect(MockMvcResultMatchers.header().string(
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

  private void givenBucket() {
    when(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
    when(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);
  }

  private S3ObjectMetadata s3ObjectMetadata(String id, String digest) {
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setName(id);
    s3ObjectMetadata.setModificationDate("1234");
    s3ObjectMetadata.setEtag(digest);
    s3ObjectMetadata.setSize("size");
    return s3ObjectMetadata;
  }

  private S3ObjectMetadata s3ObjectEncrypted(
      String id, String encryption, String encryptionKey) {
    S3ObjectMetadata s3ObjectMetadata = s3ObjectMetadata(id, "digest");
    s3ObjectMetadata.setEncrypted(true);
    s3ObjectMetadata.setKmsEncryption(encryption);
    s3ObjectMetadata.setKmsKeyId(encryptionKey);
    s3ObjectMetadata.setSize("12345");
    final File sourceFile = new File("src/test/resources/sampleFile.txt");
    s3ObjectMetadata.setDataPath(sourceFile.toPath());
    return s3ObjectMetadata;
  }
}


