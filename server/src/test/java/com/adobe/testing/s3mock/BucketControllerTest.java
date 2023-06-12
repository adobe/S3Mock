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

import static com.adobe.testing.s3mock.S3Exception.BUCKET_NOT_EMPTY;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_ENCODINGTYPE;
import static com.adobe.testing.s3mock.S3Exception.INVALID_REQUEST_MAXKEYS;
import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_BUCKET;
import static com.adobe.testing.s3mock.dto.LifecycleRule.Status.ENABLED;
import static com.adobe.testing.s3mock.dto.StorageClass.GLACIER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.OBJECT_LOCK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_XML;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.Buckets;
import com.adobe.testing.s3mock.dto.DefaultRetention;
import com.adobe.testing.s3mock.dto.ErrorResponse;
import com.adobe.testing.s3mock.dto.LifecycleExpiration;
import com.adobe.testing.s3mock.dto.LifecycleRule;
import com.adobe.testing.s3mock.dto.LifecycleRuleFilter;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.Mode;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.dto.ObjectLockEnabled;
import com.adobe.testing.s3mock.dto.ObjectLockRule;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Transition;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.KmsKeyStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.util.UriComponentsBuilder;

@MockBean(classes = {KmsKeyStore.class, ObjectService.class,
    MultipartService.class, ObjectController.class, MultipartController.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BucketControllerTest {

  private static final Owner TEST_OWNER = new Owner("123", "s3-mock-file-store");
  private static final ObjectMapper MAPPER = new XmlMapper();
  private static final String TEST_BUCKET_NAME = "test-bucket";
  private static final Bucket TEST_BUCKET =
      new Bucket(Paths.get("/tmp/foo/1"), TEST_BUCKET_NAME, Instant.now().toString());

  @MockBean
  private BucketService bucketService;

  @Autowired
  private TestRestTemplate restTemplate;

  @Test
  void testListBuckets_Ok() throws Exception {
    var bucketList = new ArrayList<Bucket>();
    bucketList.add(TEST_BUCKET);
    bucketList.add(new Bucket(Paths.get("/tmp/foo/2"), "test-bucket1", Instant.now().toString()));
    var expected = new ListAllMyBucketsResult(TEST_OWNER, new Buckets(bucketList));
    when(bucketService.listBuckets()).thenReturn(expected);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/",
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(expected));
  }

  @Test
  void testListBuckets_Empty() throws Exception {
    var expected = new ListAllMyBucketsResult(TEST_OWNER, new Buckets(Collections.emptyList()));
    when(bucketService.listBuckets()).thenReturn(expected);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/",
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(expected));
  }

  @Test
  void testHeadBucket_Ok() {
    when(bucketService.doesBucketExist(TEST_BUCKET_NAME)).thenReturn(true);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.HEAD,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void testHeadBucket_NotFound() {
    doThrow(NO_SUCH_BUCKET).when(bucketService).verifyBucketExists(anyString());

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void testCreateBucket_Ok() {
    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.PUT,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void testCreateBucket_InternalServerError() {
    when(bucketService.createBucket(TEST_BUCKET_NAME, false))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.PUT,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void testDeleteBucket_NoContent() throws Exception {
    givenBucket();
    when(bucketService.isBucketEmpty(TEST_BUCKET_NAME)).thenReturn(true);
    when(bucketService.deleteBucket(TEST_BUCKET_NAME)).thenReturn(true);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.DELETE,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
  }

  @Test
  void testDeleteBucket_NotFound() throws Exception {
    doThrow(NO_SUCH_BUCKET)
        .when(bucketService).verifyBucketIsEmpty(anyString());

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.DELETE,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(from(NO_SUCH_BUCKET)));
  }

  @Test
  void testDeleteBucket_Conflict() throws Exception {
    givenBucket();
    doThrow(BUCKET_NOT_EMPTY)
        .when(bucketService).verifyBucketIsEmpty(anyString());

    when(bucketService.getS3Objects(TEST_BUCKET_NAME, null))
        .thenReturn(Collections.singletonList(new S3Object(
            null, null, null, null, null, null
        )));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.DELETE,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(from(BUCKET_NOT_EMPTY)));
  }

  @Test
  void testDeleteBucket_InternalServerError() {
    givenBucket();

    doThrow(new IllegalStateException("THIS IS EXPECTED"))
        .when(bucketService).verifyBucketIsEmpty(anyString());

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.DELETE,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void testListObjectsV1_BadRequest() throws Exception {
    givenBucket();

    var maxKeys = -1;
    doThrow(INVALID_REQUEST_MAXKEYS).when(bucketService).verifyMaxKeys(maxKeys);
    var encodingtype = "not_valid";
    doThrow(INVALID_REQUEST_ENCODINGTYPE).when(bucketService).verifyEncodingType(encodingtype);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var maxKeysUri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(MAX_KEYS, String.valueOf(maxKeys)).build().toString();
    var maxKeysResponse = restTemplate.exchange(
        maxKeysUri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(maxKeysResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(maxKeysResponse.getBody())
        .isEqualTo(MAPPER.writeValueAsString(from(INVALID_REQUEST_MAXKEYS)));

    var encodingTypeUri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(ENCODING_TYPE, encodingtype).build().toString();
    var encodingTypeResponse = restTemplate.exchange(
        encodingTypeUri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(encodingTypeResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(encodingTypeResponse.getBody())
        .isEqualTo(MAPPER.writeValueAsString(from(INVALID_REQUEST_ENCODINGTYPE)));
  }

  @Test
  void testListObjectsV2_BadRequest() throws Exception {
    givenBucket();

    var maxKeys = -1;
    doThrow(INVALID_REQUEST_MAXKEYS).when(bucketService).verifyMaxKeys(maxKeys);
    var encodingtype = "not_valid";
    doThrow(INVALID_REQUEST_ENCODINGTYPE).when(bucketService).verifyEncodingType(encodingtype);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var maxKeysUri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam("list-type", "2")
        .queryParam(MAX_KEYS, String.valueOf(maxKeys))
        .build().toString();
    var maxKeysResponse = restTemplate.exchange(
        maxKeysUri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(maxKeysResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(maxKeysResponse.getBody())
        .isEqualTo(MAPPER.writeValueAsString(from(INVALID_REQUEST_MAXKEYS)));

    var encodingTypeUri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(ENCODING_TYPE, encodingtype)
        .queryParam("list-type", "2")
        .build().toString();
    var encodingTypeResponse = restTemplate.exchange(
        encodingTypeUri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(encodingTypeResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(encodingTypeResponse.getBody())
        .isEqualTo(MAPPER.writeValueAsString(from(INVALID_REQUEST_ENCODINGTYPE)));
  }

  @Test
  void testListObjectsV1_InternalServerError() throws Exception {
    givenBucket();
    when(bucketService.listObjectsV1(TEST_BUCKET_NAME, null, null, null, null, 1000))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket/",
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void testListObjectsV2_InternalServerError() throws Exception {
    givenBucket();
    when(bucketService.listObjectsV2(TEST_BUCKET_NAME, null, null, null, null, 1000, null))
        .thenThrow(new IllegalStateException("THIS IS EXPECTED"));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket/")
        .queryParam("list-type", "2").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void testListObjectsV1_Ok() throws Exception {
    givenBucket();
    var key = "key";
    var s3Object = bucketContents(key);
    var expected =
        new ListBucketResult(TEST_BUCKET_NAME, null, null, 1000, false, null, null,
            Collections.singletonList(s3Object), Collections.emptyList());

    when(bucketService.listObjectsV1(TEST_BUCKET_NAME, null, null, null, null, 1000))
        .thenReturn(expected);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var response = restTemplate.exchange(
        "/test-bucket",
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(expected));
  }

  @Test
  void testListObjectsV2_Ok() throws Exception {
    givenBucket();
    var key = "key";
    var s3Object = bucketContents(key);
    var expected =
        new ListBucketResultV2(TEST_BUCKET_NAME, null, 1000, false,
            Collections.singletonList(s3Object), Collections.emptyList(),
            null, null, null, null, null);

    when(bucketService.listObjectsV2(TEST_BUCKET_NAME, null, null, null, null, 1000, null))
        .thenReturn(expected);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam("list-type", "2").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(expected));
  }

  @Test
  void testPutBucketObjectLockConfiguration_Ok() throws Exception {
    givenBucket();
    var retention = new DefaultRetention(1, null, Mode.COMPLIANCE);
    var rule = new ObjectLockRule(retention);
    var expected = new ObjectLockConfiguration(ObjectLockEnabled.ENABLED, rule);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(OBJECT_LOCK, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(expected), headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

    verify(bucketService).setObjectLockConfiguration(eq(TEST_BUCKET_NAME), eq(expected));
  }

  @Test
  void testGetBucketObjectLockConfiguration_Ok() throws Exception {
    givenBucket();
    var retention = new DefaultRetention(1, null, Mode.COMPLIANCE);
    var rule = new ObjectLockRule(retention);
    var expected = new ObjectLockConfiguration(ObjectLockEnabled.ENABLED, rule);

    when(bucketService.getObjectLockConfiguration(eq(TEST_BUCKET_NAME))).thenReturn(expected);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(OBJECT_LOCK, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(expected));
  }

  @Test
  void testPutBucketLifecycleConfiguration_Ok() throws Exception {
    givenBucket();

    var filter1 = new LifecycleRuleFilter(null, null, "documents/", null, null);
    var transition1 = new Transition(null, 30, GLACIER);
    var rule1 = new LifecycleRule(null, null, filter1, "id1", null, null,
        ENABLED, Collections.singletonList(transition1));
    var filter2 = new LifecycleRuleFilter(null, null, "logs/", null, null);
    var expiration2 = new LifecycleExpiration(null, 365, null);
    var rule2 = new LifecycleRule(null, expiration2, filter2, "id2", null, null,
        ENABLED, null);
    var configuration =
        new BucketLifecycleConfiguration(Arrays.asList(rule1, rule2));

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(LIFECYCLE, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.PUT,
        new HttpEntity<>(MAPPER.writeValueAsString(configuration), headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

    verify(bucketService).setBucketLifecycleConfiguration(eq(TEST_BUCKET_NAME), eq(configuration));
  }

  @Test
  void testGetBucketLifecycleConfiguration_Ok() throws Exception {
    givenBucket();

    var filter1 = new LifecycleRuleFilter(null, null, "documents/", null, null);
    var transition1 = new Transition(null, 30, GLACIER);
    var rule1 = new LifecycleRule(null, null, filter1, "id1", null, null,
        ENABLED, Collections.singletonList(transition1));
    var filter2 = new LifecycleRuleFilter(null, null, "logs/", null, null);
    var expiration2 = new LifecycleExpiration(null, 365, null);
    var rule2 = new LifecycleRule(null, expiration2, filter2, "id2", null, null,
        ENABLED, null);
    var configuration =
        new BucketLifecycleConfiguration(Arrays.asList(rule1, rule2));

    when(bucketService.getBucketLifecycleConfiguration(eq(TEST_BUCKET_NAME))).thenReturn(
        configuration);

    var headers = new HttpHeaders();
    headers.setAccept(List.of(APPLICATION_XML));
    headers.setContentType(APPLICATION_XML);
    var uri = UriComponentsBuilder.fromUriString("/test-bucket")
        .queryParam(LIFECYCLE, "ignored").build().toString();
    var response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        new HttpEntity<>(headers),
        String.class
    );
    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(response.getBody()).isEqualTo(MAPPER.writeValueAsString(configuration));
  }

  private S3Object bucketContents(String id) {
    return new S3Object(id, "1234", "etag", "size", StorageClass.STANDARD, TEST_OWNER);
  }

  private void givenBucket() {
    when(bucketService.getBucket(TEST_BUCKET_NAME)).thenReturn(TEST_BUCKET);
  }

  private ErrorResponse from(S3Exception e) {
    return new ErrorResponse(
        e.getCode(),
        e.getMessage(),
        null,
        null
    );
  }
}
