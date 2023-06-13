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

package com.adobe.testing.s3mock.store;

import static com.adobe.testing.s3mock.store.StoreConfiguration.S3_OBJECT_DATE_FORMAT;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.CONTENT_ENCODING;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.adobe.testing.s3mock.dto.Owner;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

@MockBean(classes = {KmsKeyStore.class})
@SpringBootTest(classes = {StoreConfiguration.class,
    StoresWithExistingFileRootTest.TestConfig.class},
    webEnvironment = SpringBootTest.WebEnvironment.NONE)
class StoresWithExistingFileRootTest extends StoreTestBase {
  @Autowired
  private BucketStore bucketStore;
  @Autowired
  private BucketStore testBucketStore;

  @Autowired
  private ObjectStore objectStore;

  @Autowired
  private ObjectStore testObjectStore;

  @Test
  void testBucketStoreWithExistingRoot() {
    bucketStore.createBucket(TEST_BUCKET_NAME, false);
    var bucket = bucketStore.getBucketMetadata(TEST_BUCKET_NAME);

    assertThatThrownBy(() ->
        testBucketStore.getBucketMetadata(TEST_BUCKET_NAME)
    ).isInstanceOf(NullPointerException.class);

    testBucketStore.loadBuckets(Collections.singletonList(TEST_BUCKET_NAME));
    var reloadedBucket = testBucketStore.getBucketMetadata(TEST_BUCKET_NAME);
    assertThat(reloadedBucket.creationDate()).isEqualTo(bucket.creationDate());
    assertThat(reloadedBucket.path()).isEqualTo(bucket.path());
  }

  @Test
  void testObjectStoreWithExistingRoot() throws IOException {
    var sourceFile = new File(TEST_FILE_PATH);
    var path = sourceFile.toPath();
    var id = UUID.randomUUID();
    var name = sourceFile.getName();
    var bucketMetadata = metadataFrom(TEST_BUCKET_NAME);
    objectStore
        .storeS3ObjectMetadata(bucketMetadata, id, name, TEXT_PLAIN, storeHeaders(),
            Files.newInputStream(path), false,
            emptyMap(), emptyMap(), null, emptyList(), Owner.DEFAULT_OWNER);

    var object = objectStore.getS3ObjectMetadata(bucketMetadata, id);

    assertThatThrownBy(() ->
        testObjectStore.getS3ObjectMetadata(bucketMetadata, id)
    ).isInstanceOf(NullPointerException.class);

    testObjectStore.loadObjects(bucketMetadata, Collections.singletonList(object.id()));

    var reloadedObject = testObjectStore.getS3ObjectMetadata(bucketMetadata, id);
    assertThat(reloadedObject.modificationDate()).isEqualTo(object.modificationDate());
    assertThat(reloadedObject.etag()).isEqualTo(object.etag());
  }

  @TestConfiguration
  protected static class TestConfig {
    @Bean
    BucketStore testBucketStore(StoreProperties properties, File rootFolder,
        ObjectMapper objectMapper) {
      return new BucketStore(rootFolder, properties.retainFilesOnExit(),
          S3_OBJECT_DATE_FORMAT, objectMapper);
    }

    @Bean
    ObjectStore testObjectStore(StoreProperties properties, ObjectMapper objectMapper) {
      return new ObjectStore(properties.retainFilesOnExit(),
          S3_OBJECT_DATE_FORMAT, objectMapper);
    }

    @Bean
    ObjectMapper objectMapper() {
      return new ObjectMapper();
    }
  }

  private Map<String, String> storeHeaders() {
    Map<String, String> storeHeaders = new HashMap<>();
    storeHeaders.put(CONTENT_ENCODING, ENCODING_GZIP);
    return storeHeaders;
  }
}
