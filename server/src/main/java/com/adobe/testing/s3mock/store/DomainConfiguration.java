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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(DomainProperties.class)
class DomainConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(DomainConfiguration.class);
  private static final DateTimeFormatter S3_OBJECT_DATE_FORMAT = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .withZone(ZoneId.of("UTC"));

  @Bean
  FileStore fileStore(DomainProperties properties, BucketStore bucketStore,
      ObjectMapper objectMapper) {
    return new FileStore(properties.isRetainFilesOnExit(),
        bucketStore, S3_OBJECT_DATE_FORMAT, objectMapper);
  }

  @Bean
  BucketStore bucketStore(DomainProperties properties, File bucketRootFolder) {
    return new BucketStore(bucketRootFolder, properties.isRetainFilesOnExit(),
        properties.getInitialBuckets(), S3_OBJECT_DATE_FORMAT);
  }

  @Bean
  KmsKeyStore kmsKeyStore(DomainProperties properties) {
    return new KmsKeyStore(properties.getValidKmsKeys());
  }

  @Bean
  File bucketRootFolder(File rootFolder) {
    return rootFolder;
  }

  @Bean
  File rootFolder(DomainProperties properties) {
    final File root;
    if (properties.getRoot() == null || properties.getRoot().isEmpty()) {
      root = new File(FileUtils.getTempDirectory(), "s3mockFileStore" + new Date().getTime());
    } else {
      root = new File(properties.getRoot());
    }
    if (!properties.isRetainFilesOnExit()) {
      root.deleteOnExit();
    }
    if (!root.mkdir()) {
      throw new IllegalStateException("Root folder could not be created. Path: "
          + root.getAbsolutePath());
    }

    LOG.info("Using \"{}\" as root folder. Will retain files on exit: {}",
        root.getAbsolutePath(), properties.isRetainFilesOnExit());

    return root;
  }
}
