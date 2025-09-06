/*
 *  Copyright 2017-2025 Adobe.
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

import static com.adobe.testing.s3mock.store.BucketStore.BUCKET_META_FILE;

import com.adobe.testing.s3mock.dto.ObjectOwnership;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;

@Configuration
@EnableConfigurationProperties({StoreProperties.class, LegacyStoreProperties.class})
public class StoreConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(StoreConfiguration.class);
  static final DateTimeFormatter S3_OBJECT_DATE_FORMAT = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .withZone(ZoneId.of("UTC"));

  @Bean
  ObjectStore objectStore(
      List<String> bucketNames,
      BucketStore bucketStore,
      ObjectMapper objectMapper) {
    var objectStore = new ObjectStore(S3_OBJECT_DATE_FORMAT, objectMapper);
    for (var bucketName : bucketNames) {
      var bucketMetadata = bucketStore.getBucketMetadata(bucketName);
      if (bucketMetadata != null) {
        objectStore.loadObjects(bucketMetadata, bucketMetadata.objects().values());
      }
    }
    return objectStore;
  }

  @Bean
  BucketStore bucketStore(
      StoreProperties properties,
      LegacyStoreProperties legacyProperties,
      File rootFolder,
      List<String> bucketNames,
      ObjectMapper objectMapper,
      @Nullable @Value("${com.adobe.testing.s3mock.region}") Region region) {
    Region mockRegion = region == null ? properties.region() : region;

    var bucketStore = new BucketStore(rootFolder, S3_OBJECT_DATE_FORMAT, mockRegion.id(), objectMapper);
    // load existing buckets first
    bucketStore.loadBuckets(bucketNames);

    // load initialBuckets if not part of existing buckets
    List<String> initialBuckets = List.of();
    if (!legacyProperties.initialBuckets().isEmpty()) {
      initialBuckets = legacyProperties.initialBuckets();
    } else if (!properties.initialBuckets().isEmpty()) {
      initialBuckets = properties.initialBuckets();
    }

    initialBuckets
        .stream()
        .filter(name -> {
          boolean partOfExistingBuckets = bucketNames.contains(name);
          if (partOfExistingBuckets) {
            LOG.info("Skip initial bucket {}, it's part of the existing buckets.", name);
          }
          return !partOfExistingBuckets;
        })
        .forEach(name -> {
          bucketStore.createBucket(name,
              false,
              ObjectOwnership.BUCKET_OWNER_ENFORCED,
              mockRegion.id(),
              null,
              null
          );
          LOG.info("Creating initial bucket {}.", name);
        });

    return bucketStore;
  }

  @Bean
  List<String> bucketNames(File rootFolder) {
    var bucketNames = new ArrayList<String>();
    try (var paths = Files.newDirectoryStream(rootFolder.toPath())) {
      paths.forEach(
          path -> {
            var resolved = path.resolve(BUCKET_META_FILE);
            if (resolved.toFile().exists()) {
              bucketNames.add(path.getFileName().toString());
            } else {
              LOG.warn("Found bucket folder {} without {}", path, BUCKET_META_FILE);
            }
          }
      );
    } catch (IOException e) {
      throw new IllegalStateException("Could not load buckets from data directory "
          + rootFolder, e);
    }
    return bucketNames;
  }

  @Bean
  MultipartStore multipartStore(
      ObjectStore objectStore,
      ObjectMapper objectMapper) {
    return new MultipartStore(objectStore, objectMapper);
  }

  @Bean
  KmsKeyStore kmsKeyStore(
      StoreProperties properties,
      LegacyStoreProperties legacyProperties) {
    if (!properties.validKmsKeys().isEmpty()) {
      return new KmsKeyStore(properties.validKmsKeys());
    } else if (!legacyProperties.validKmsKeys().isEmpty()) {
      return new KmsKeyStore(legacyProperties.validKmsKeys());
    }

    return new KmsKeyStore(new HashSet<>());
  }

  @Bean
  File rootFolder(
      StoreProperties properties,
      LegacyStoreProperties legacyProperties) {
    File root;
    String rootPath = null;
    if (legacyProperties.root() != null && !legacyProperties.root().isEmpty()) {
      rootPath = legacyProperties.root();
    } else if (properties.root() != null && !properties.root().isEmpty()) {
      rootPath = properties.root();
    }

    var createTempDir = rootPath == null;

    if (createTempDir) {
      var baseTempDir = FileUtils.getTempDirectory().toPath();
      try {
        root = Files.createTempDirectory(baseTempDir, "s3mockFileStore").toFile();
      } catch (IOException e) {
        throw new IllegalStateException("Root folder could not be created. Base temp dir: "
            + baseTempDir, e);
      }

      LOG.info("Successfully created \"{}\" as root folder. Will retain files on exit: {}",
          root.getAbsolutePath(), properties.retainFilesOnExit());
    } else {
      root = new File(rootPath);

      if (root.exists()) {
        LOG.info("Using existing folder \"{}\" as root folder. Will retain files on exit: {}",
            root.getAbsolutePath(), properties.retainFilesOnExit());
        // TODO: need to validate folder structure here?
      } else if (!root.mkdir()) {
        throw new IllegalStateException("Root folder could not be created. Path: "
            + root.getAbsolutePath());
      } else {
        LOG.info("Successfully created \"{}\" as root folder. Will retain files on exit: {}",
            root.getAbsolutePath(), properties.retainFilesOnExit());
      }
    }
    return root;
  }

  @Bean
  StoreCleaner storeCleaner(
      File rootFolder,
      StoreProperties properties,
      LegacyStoreProperties legacyProperties) {
    if (legacyProperties.retainFilesOnExit() || properties.retainFilesOnExit()) {
      return new StoreCleaner(rootFolder, true);
    } else {
      return new StoreCleaner(rootFolder, false);
    }
  }
}
