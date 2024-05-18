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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreConfigurationTest {
  private static final String BUCKET_META_FILE = "bucketMetadata.json";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void bucketCreation_noExistingBuckets(@TempDir Path tempDir) throws IOException {
    var initialBucketName = "initialBucketName";

    var properties = new StoreProperties(false, null, Set.of(), List.of(initialBucketName));
    var iut = new StoreConfiguration();
    var bucketStore = iut.bucketStore(properties, tempDir.toFile(), List.of(), OBJECT_MAPPER);
    assertThat(bucketStore.getBucketMetadata(initialBucketName).name())
        .isEqualTo(initialBucketName);

    var createdBuckets = new ArrayList<Path>();
    try (var paths = Files.newDirectoryStream(tempDir)) {
      paths.forEach(
          createdBuckets::add
      );
    }
    assertThat(createdBuckets).hasSize(1);
    assertThat(createdBuckets.get(0).getFileName()).hasToString(initialBucketName);
    assertThat(bucketStore.getBucketMetadata(initialBucketName).path())
        .isEqualTo(createdBuckets.get(0));
  }


  @Test
  void bucketCreation_existingBuckets(@TempDir Path tempDir) throws IOException {
    var existingBucketName = "existingBucketName";
    var existingBucket = Paths.get(tempDir.toAbsolutePath().toString(), existingBucketName);
    FileUtils.forceMkdir(existingBucket.toFile());
    var bucketMetadata =
        new BucketMetadata(existingBucketName, Instant.now().toString(),
            null, null, existingBucket);
    Path metaFile = Paths.get(existingBucket.toString(), BUCKET_META_FILE);
    OBJECT_MAPPER.writeValue(metaFile.toFile(), bucketMetadata);

    var initialBucketName = "initialBucketName";

    var properties = new StoreProperties(false, null, Set.of(), List.of(initialBucketName));
    var iut = new StoreConfiguration();
    var bucketStore =
        iut.bucketStore(properties, tempDir.toFile(), List.of(existingBucketName), OBJECT_MAPPER);

    assertThat(bucketStore.getBucketMetadata(initialBucketName).name())
        .isEqualTo(initialBucketName);

    var createdBuckets = new ArrayList<Path>();
    try (var paths = Files.newDirectoryStream(tempDir)) {
      paths.forEach(
          createdBuckets::add
      );
    }
    assertThat(createdBuckets)
        .hasSize(2)
        .containsAll(List.of(bucketStore.getBucketMetadata(existingBucketName).path(),
            bucketStore.getBucketMetadata(initialBucketName).path())
        )
        .extracting(Path::getFileName)
        .containsAll(List.of(Path.of(existingBucketName), Path.of(initialBucketName)));
  }
}
