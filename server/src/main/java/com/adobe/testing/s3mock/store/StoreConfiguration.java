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

package com.adobe.testing.s3mock.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(StoreProperties.class)
public class StoreConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(StoreConfiguration.class);
  static final DateTimeFormatter S3_OBJECT_DATE_FORMAT = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .withZone(ZoneId.of("UTC"));

  @Bean
  ObjectStore objectStore(StoreProperties properties, List<String> bucketNames,
      BucketStore bucketStore, ObjectMapper objectMapper) {
    ObjectStore objectStore = new ObjectStore(properties.isRetainFilesOnExit(),
        S3_OBJECT_DATE_FORMAT, objectMapper);
    for (String bucketName : bucketNames) {
      BucketMetadata bucketMetadata = bucketStore.getBucketMetadata(bucketName);
      if (bucketMetadata != null) {
        objectStore.loadObjects(bucketMetadata, bucketMetadata.getObjects().values());
      }
    }
    return objectStore;
  }

  @Bean
  BucketStore bucketStore(StoreProperties properties, File rootFolder, List<String> bucketNames,
      ObjectMapper objectMapper) {
    BucketStore bucketStore = new BucketStore(rootFolder, properties.isRetainFilesOnExit(),
        S3_OBJECT_DATE_FORMAT, objectMapper);
    if (bucketNames.isEmpty()) {
      properties
          .getInitialBuckets()
          .forEach(bucketName -> bucketStore.createBucket(bucketName, false));
    } else {
      bucketStore.loadBuckets(bucketNames);
    }

    return bucketStore;
  }

  @Bean
  List<String> bucketNames(File rootFolder) {
    File[] buckets = rootFolder.listFiles((File dir, String name) -> !Objects.equals(name, "test"));
    if (buckets != null) {
      return Arrays.stream(buckets).map(File::getName).collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  @Bean
  MultipartStore multipartStore(StoreProperties properties, ObjectStore objectStore) {
    return new MultipartStore(properties.isRetainFilesOnExit(), objectStore);
  }

  @Bean
  KmsKeyStore kmsKeyStore(StoreProperties properties) {
    return new KmsKeyStore(properties.getValidKmsKeys());
  }

  @Bean
  File rootFolder(StoreProperties properties) {
    final File root;
    final boolean createTempDir = properties.getRoot() == null || properties.getRoot().isEmpty();

    if (createTempDir) {
      final Path baseTempDir = FileUtils.getTempDirectory().toPath();
      try {
        root = Files.createTempDirectory(baseTempDir, "s3mockFileStore").toFile();
      } catch (IOException e) {
        throw new IllegalStateException("Root folder could not be created. Base temp dir: "
            + baseTempDir, e);
      }

      LOG.info("Successfully created \"{}\" as root folder. Will retain files on exit: {}",
          root.getAbsolutePath(), properties.isRetainFilesOnExit());
    } else {
      root = new File(properties.getRoot());

      if (root.exists()) {
        LOG.info("Using existing folder \"{}\" as root folder. Will retain files on exit: {}",
            root.getAbsolutePath(), properties.isRetainFilesOnExit());
        //TODO: need to validate folder structure here?
      } else if (!root.mkdir()) {
        throw new IllegalStateException("Root folder could not be created. Path: "
            + root.getAbsolutePath());
      } else {
        LOG.info("Successfully created \"{}\" as root folder. Will retain files on exit: {}",
            root.getAbsolutePath(), properties.isRetainFilesOnExit());
      }
    }

    if (!properties.isRetainFilesOnExit()) {
      root.deleteOnExit();
    }

    return root;
  }
}
