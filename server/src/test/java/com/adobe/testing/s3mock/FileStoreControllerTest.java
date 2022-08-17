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

import static com.adobe.testing.s3mock.FileStoreController.collapseCommonPrefixes;
import static com.adobe.testing.s3mock.FileStoreController.filterBucketContentsBy;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadRequest;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@AutoConfigureWebMvc
@AutoConfigureMockMvc
@SpringBootTest(classes = {S3MockConfiguration.class})
class FileStoreControllerTest {
  //verbatim copy from FileStoreController / FileStore
  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");
  private static final ObjectMapper MAPPER = new XmlMapper();
  private static final String[] ALL_OBJECTS =
      new String[] {"3330/0", "33309/0", "a",
          "b", "b/1", "b/1/1", "b/1/2", "b/2",
          "c/1", "c/1/1",
          "d:1", "d:1:1",
          "eor.txt", "foo/eor.txt"};

  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());
  private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  @MockBean
  private KmsKeyStore kmsKeyStore; //Dependency of S3MockConfiguration.

  @MockBean
  private FileStore fileStore;
  @MockBean
  private BucketStore bucketStore;

  @Autowired
  private MockMvc mockMvc;

  @Test
  void testListBuckets_Ok() throws Exception {
    List<Bucket> bucketList = new ArrayList<>();
    bucketList.add(TEST_BUCKET);
    bucketList.add(new Bucket(Paths.get("/tmp/foo/2"), "test-bucket1", Instant.now().toString()));
    when(bucketStore.listBuckets()).thenReturn(bucketList);
    ListAllMyBucketsResult expected = new ListAllMyBucketsResult(TEST_OWNER, bucketList);

    mockMvc.perform(
            get("/")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  @Test
  void testListBuckets_Empty() throws Exception {
    when(bucketStore.listBuckets()).thenReturn(Collections.emptyList());

    ListAllMyBucketsResult expected =
        new ListAllMyBucketsResult(TEST_OWNER, Collections.emptyList());

    mockMvc.perform(
            get("/")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  @Test
  void testHeadBucket_Ok() throws Exception {
    when(bucketStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        head("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testHeadBucket_NotFound() throws Exception {
    when(bucketStore.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(false);

    mockMvc.perform(
        head("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testCreateBucket_Ok() throws Exception {
    mockMvc.perform(
        put("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isOk());
  }

  @Test
  void testCreateBucket_InternalServerError() throws Exception {
    when(bucketStore.createBucket(TEST_BUCKET_NAME))
        .thenThrow(new RuntimeException("THIS IS EXPECTED"));

    mockMvc.perform(
        put("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testDeleteBucket_NoContent() throws Exception {
    givenBucket();

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null)).thenReturn(Collections.emptyList());

    when(bucketStore.deleteBucket(TEST_BUCKET_NAME)).thenReturn(true);

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNoContent());
  }

  @Test
  void testDeleteBucket_NotFound() throws Exception {
    givenBucket();

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null)).thenReturn(Collections.emptyList());

    when(bucketStore.deleteBucket(TEST_BUCKET_NAME)).thenReturn(false);

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isNotFound());
  }

  @Test
  void testDeleteBucket_Conflict() throws Exception {
    givenBucket();

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null))
        .thenReturn(Collections.singletonList(new S3ObjectMetadata()));

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isConflict());
  }

  @Test
  void testDeleteBucket_InternalServerError() throws Exception {
    givenBucket();

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, null))
        .thenThrow(new IOException("THIS IS EXPECTED"));

    mockMvc.perform(
        delete("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testListObjectsInsideBucket_BadRequest() throws Exception {
    givenBucket();

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
            .queryParam(MAX_KEYS, "-1")
    ).andExpect(MockMvcResultMatchers.status().isBadRequest());

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
            .queryParam(ENCODING_TYPE, "not_valid")
    ).andExpect(MockMvcResultMatchers.status().isBadRequest());
  }

  @Test
  void testListObjectsInsideBucket_InternalServerError() throws Exception {
    givenBucket();
    String prefix = null;
    when(fileStore.getS3Objects(TEST_BUCKET_NAME, prefix))
        .thenThrow(new IOException("THIS IS EXPECTED"));

    mockMvc.perform(
        get("/test-bucket")
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.APPLICATION_XML)
    ).andExpect(MockMvcResultMatchers.status().isInternalServerError());
  }

  @Test
  void testListObjectsInsideBucket_Ok() throws Exception {
    givenBucket();
    String key = "key";
    String prefix = null;
    S3Object s3Object = bucketContents(key);
    ListBucketResult expected =
        new ListBucketResult(TEST_BUCKET_NAME, null, null, 1000, false, null, null,
            Collections.singletonList(s3Object), Collections.emptyList());

    when(fileStore.getS3Objects(TEST_BUCKET_NAME, prefix))
        .thenReturn(Collections.singletonList(s3Object(key, "etag")));

    mockMvc.perform(
            get("/test-bucket")
                .accept(MediaType.APPLICATION_XML)
                .contentType(MediaType.APPLICATION_XML)
        ).andExpect(MockMvcResultMatchers.status().isOk())
        .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_XML))
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(expected)));
  }

  @Test
  void testPutObject_Ok() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";

    File testFile = new File(UPLOAD_FILE_NAME);
    String digest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));

    when(fileStore.putS3Object(eq(TEST_BUCKET_NAME), eq(key), contains(MediaType.TEXT_PLAIN_VALUE),
        isNull(),
        any(InputStream.class), eq(false), anyMap(), isNull(), isNull()))
        .thenReturn(s3Object(key, digest));

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

    when(fileStore.putS3Object(eq(TEST_BUCKET_NAME), eq(key), contains(MediaType.TEXT_PLAIN_VALUE),
        isNull(),
        any(InputStream.class), eq(false), anyMap(), isNull(), isNull()))
        .thenReturn(s3Object(key, hexDigest));

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
    String hexDigest = DigestUtil.hexDigest(FileUtils.openInputStream(testFile));
    String base64Digest = DigestUtil.base64Digest(FileUtils.openInputStream(testFile));

    when(fileStore.putS3Object(eq(TEST_BUCKET_NAME), eq(key), contains(MediaType.TEXT_PLAIN_VALUE),
        isNull(),
        any(InputStream.class), eq(false), anyMap(), isNull(), isNull()))
        .thenReturn(s3Object(key, hexDigest));

    mockMvc.perform(
        put("/test-bucket/" + key)
            .accept(MediaType.APPLICATION_XML)
            .contentType(MediaType.TEXT_PLAIN_VALUE)
            .content(FileUtils.readFileToByteArray(testFile))
            .header(CONTENT_MD5, base64Digest + 1)
    ).andExpect(MockMvcResultMatchers.status().isBadRequest());
  }

  @Test
  void testCompleteMultipart_BadRequest_uploadTooSmall() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";
    String uploadId = "testUploadId";

    List<Part> parts = new ArrayList<>();
    parts.add(createPart(0, 5L));
    parts.add(createPart(1, 5L));

    when(fileStore.getMultipartUploadParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId)))
        .thenReturn(parts);

    CompleteMultipartUploadRequest uploadRequest = new CompleteMultipartUploadRequest();
    for (Part part : parts) {
      uploadRequest.setPart(part);
    }
    ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode("EntityTooSmall");
    errorResponse.setMessage("Your proposed upload is smaller than the minimum allowed object size."
        + " Each part must be at least 5 MB in size, except the last part.");

    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isBadRequest())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testCompleteMultipart_BadRequest_uploadIdNotFound() throws Exception {
    givenBucket();
    String uploadId = "testUploadId";

    List<Part> parts = new ArrayList<>();
    parts.add(createPart(0, 5L));
    parts.add(createPart(1, 5L));

    when(fileStore.getMultipartUpload(eq(uploadId)))
        .thenThrow(IllegalArgumentException.class);

    CompleteMultipartUploadRequest uploadRequest = new CompleteMultipartUploadRequest();
    for (Part part : parts) {
      uploadRequest.setPart(part);
    }
    ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode("NoSuchUpload");
    errorResponse.setMessage(
        "The specified multipart upload does not exist. The upload ID might be "
            + "invalid, or the multipart upload might have been aborted or completed.");

    String key = "sampleFile.txt";
    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isNotFound())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testCompleteMultipart_BadRequest_partNotFound() throws Exception {
    givenBucket();
    String key = "sampleFile.txt";
    String uploadId = "testUploadId";

    List<Part> uploadedParts = new ArrayList<>();
    uploadedParts.add(createPart(0, 5L));

    List<Part> requestParts = new ArrayList<>();
    requestParts.add(createPart(1, 5L));

    when(fileStore.getMultipartUploadParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId)))
        .thenReturn(uploadedParts);

    CompleteMultipartUploadRequest uploadRequest = new CompleteMultipartUploadRequest();
    for (Part part : requestParts) {
      uploadRequest.setPart(part);
    }
    ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode("InvalidPart");
    errorResponse.setMessage(
        "One or more of the specified parts could not be found. The part might "
            + "not have been uploaded, or the specified entity tag might not have matched the "
            + "part's "
            + "entity tag.");

    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isBadRequest())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testCompleteMultipart_BadRequest_invalidPartOrder() throws Exception {
    givenBucket();

    List<Part> uploadedParts = new ArrayList<>();
    uploadedParts.add(createPart(0, 10000000L));
    uploadedParts.add(createPart(1, 10000000L));

    String key = "sampleFile.txt";
    String uploadId = "testUploadId";

    when(fileStore.getMultipartUploadParts(eq(TEST_BUCKET_NAME), eq(key), eq(uploadId)))
        .thenReturn(uploadedParts);

    List<Part> requestParts = new ArrayList<>();
    requestParts.add(createPart(1, 5L));
    requestParts.add(createPart(0, 5L));

    CompleteMultipartUploadRequest uploadRequest = new CompleteMultipartUploadRequest();
    for (Part part : requestParts) {
      uploadRequest.setPart(part);
    }
    ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode("InvalidPartOrder");
    errorResponse.setMessage("The list of parts was not in ascending order. The parts list must be "
        + "specified in order by part number.");

    mockMvc.perform(
            post("/test-bucket/" + key)
                .accept(MediaType.APPLICATION_XML)
                .content(MAPPER.writeValueAsString(uploadRequest))
                .param("uploadId", uploadId)
        ).andExpect(MockMvcResultMatchers.status().isBadRequest())
        .andExpect(MockMvcResultMatchers.content().xml(MAPPER.writeValueAsString(errorResponse)));
  }

  @Test
  void testGetObject_Encrypted_Ok() throws Exception {
    String encryption = "aws:kms";
    String encryptionKey = "key-ref";
    String key = "name";
    S3ObjectMetadata expectedS3ObjectMetadata = s3ObjectEncrypted(key, encryption, encryptionKey);

    givenBucket();
    when(fileStore.getS3Object(any(), any())).thenReturn(expectedS3ObjectMetadata);

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
    when(fileStore.getS3Object(any(), any())).thenReturn(expectedS3ObjectMetadata);

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

  private Part createPart(int partNumber, long size) {
    Part part = new Part();
    part.setPartNumber(partNumber);
    part.setSize(size);
    part.setLastModified(new Date());
    part.setETag("someEtag" + partNumber);
    return part;
  }

  private void givenBucket() {
    when(bucketStore.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
  }

  private S3Object bucketContents(String id) {
    return new S3Object(id, "1234", "etag", "size", StorageClass.STANDARD, TEST_OWNER);
  }

  private S3ObjectMetadata s3Object(String id, String digest) {
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setName(id);
    s3ObjectMetadata.setModificationDate("1234");
    s3ObjectMetadata.setEtag(digest);
    s3ObjectMetadata.setSize("size");
    return s3ObjectMetadata;
  }

  private S3ObjectMetadata s3ObjectEncrypted(
      String id, String encryption, String encryptionKey) {
    S3ObjectMetadata s3ObjectMetadata = s3Object(id, "digest");
    s3ObjectMetadata.setEncrypted(true);
    s3ObjectMetadata.setKmsEncryption(encryption);
    s3ObjectMetadata.setKmsKeyId(encryptionKey);
    s3ObjectMetadata.setSize("12345");
    final File sourceFile = new File("src/test/resources/sampleFile.txt");
    s3ObjectMetadata.setDataPath(sourceFile.toPath());
    return s3ObjectMetadata;
  }

  /**
   * Parameter factory.
   * Taken from ListObjectIT to make sure we unit test against the same data.
   */
  public static Iterable<Param> data() {
    return Arrays.asList(
        param(null, null).keys(ALL_OBJECTS),
        param("", null).keys(ALL_OBJECTS),
        param(null, "").keys(ALL_OBJECTS),
        param(null, "/").keys("a", "b", "d:1", "d:1:1", "eor.txt")
            .prefixes("3330/", "foo/", "c/", "b/", "33309/"),
        param("", "").keys(ALL_OBJECTS),
        param("/", null),
        param("b", null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),
        param("b/", null).keys("b/1", "b/1/1", "b/1/2", "b/2"),
        param("b", "").keys("b", "b/1", "b/1/1", "b/1/2", "b/2"),
        param("b", "/").keys("b").prefixes("b/"),
        param("b/", "/").keys("b/1", "b/2").prefixes("b/1/"),
        param("b/1", "/").keys("b/1").prefixes("b/1/"),
        param("b/1/", "/").keys("b/1/1", "b/1/2"),
        param("c", "/").prefixes("c/"),
        param("c/", "/").keys("c/1").prefixes("c/1/"),
        param("eor", "/").keys("eor.txt")
    );
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCommonPrefixesAndBucketContentFilter(final Param parameters) {
    String prefix = parameters.prefix;
    String delimiter = parameters.delimiter;
    List<S3Object> bucketContents = createBucketContentsList(prefix);
    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);

    List<S3Object> filteredBucketContents =
        filterBucketContentsBy(bucketContents, commonPrefixes);

    String[] expectedPrefixes = parameters.expectedPrefixes;
    String[] expectedKeys = parameters.expectedKeys;

    assertThat(commonPrefixes).hasSize(expectedPrefixes.length);

    assertThat(commonPrefixes)
        .as("Returned prefixes are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedPrefixes));

    assertThat(filteredBucketContents.stream().map(S3Object::getKey).collect(toList()))
        .as("Returned keys are correct")
        .containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedKeys));
  }

  @Test
  void testCommonPrefixesNoPrefixNoDelimiter() {
    String prefix = "";
    String delimiter = "";
    List<S3Object> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesPrefixNoDelimiter() {
    String prefix = "prefixa";
    String delimiter = "";
    List<S3Object> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(0);
  }

  @Test
  void testCommonPrefixesNoPrefixDelimiter() {
    String prefix = "";
    String delimiter = "/";
    List<S3Object> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(5).contains("3330/", "foo/", "c/", "b/", "33309/");
  }

  @Test
  void testCommonPrefixesPrefixDelimiter() {
    String prefix = "3330";
    String delimiter = "/";
    List<S3Object> bucketContents = createBucketContentsList();

    Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, bucketContents);
    assertThat(commonPrefixes).hasSize(2).contains("3330/", "33309/");
  }

  List<S3Object> createBucketContentsList() {
    return createBucketContentsList(null);

  }

  List<S3Object> createBucketContentsList(String prefix) {
    List<S3Object> list = new ArrayList<>();
    for (String object : ALL_OBJECTS) {
      if (StringUtils.isNotEmpty(prefix)) {
        if (!object.startsWith(prefix)) {
          continue;
        }
      }
      list.add(createBucketContents(object));
    }
    return list;
  }

  S3Object createBucketContents(String key) {
    String lastModified = "lastModified";
    String etag = "etag";
    String size = "size";
    Owner owner = new Owner(0L, "name");
    return new S3Object(key, lastModified, etag, size, StorageClass.STANDARD, owner);
  }

  static class Param {
    final String prefix;
    final String delimiter;
    String[] expectedPrefixes = new String[0];
    String[] expectedKeys = new String[0];

    private Param(final String prefix, final String delimiter) {
      this.prefix = prefix;
      this.delimiter = delimiter;
    }

    Param prefixes(final String... expectedPrefixes) {
      this.expectedPrefixes = expectedPrefixes;
      return this;
    }

    Param keys(final String... expectedKeys) {
      this.expectedKeys = expectedKeys;
      return this;
    }

    @Override
    public String toString() {
      return String.format("prefix=%s, delimiter=%s", prefix, delimiter);
    }
  }

  static Param param(final String prefix, final String delimiter) {
    return new Param(prefix, delimiter);
  }
}


