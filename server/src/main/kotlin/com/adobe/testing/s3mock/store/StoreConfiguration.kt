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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.store.BucketStore.Companion.BUCKET_META_FILE
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.regions.Region
import tools.jackson.databind.ObjectMapper
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.io.path.listDirectoryEntries

@Configuration
@EnableConfigurationProperties(StoreProperties::class)
class StoreConfiguration {
  @Bean
  fun objectStore(
    bucketNames: MutableList<String>,
    bucketStore: BucketStore,
    objectMapper: ObjectMapper
  ): ObjectStore {
    val objectStore = ObjectStore(S3_OBJECT_DATE_FORMAT, objectMapper)
    for (bucketName in bucketNames) {
      val bucketMetadata = bucketStore.getBucketMetadata(bucketName)
      objectStore.loadObjects(bucketMetadata, bucketMetadata.objects.values)
    }
    return objectStore
  }

  @Bean
  fun bucketStore(
    properties: StoreProperties,
    rootFolder: File,
    bucketNames: List<String>,
    objectMapper: ObjectMapper,
    @Value($$"${com.adobe.testing.s3mock.store.region}") region: Region?
  ): BucketStore {
    val mockRegion = region ?: properties.region

    val bucketStore = BucketStore(rootFolder, S3_OBJECT_DATE_FORMAT, mockRegion.id(), objectMapper)
    // load existing buckets first
    bucketStore.loadBuckets(bucketNames)

    // load initialBuckets if not part of existing buckets
    properties.initialBuckets
      .stream()
      .filter { name: String? ->
        val partOfExistingBuckets = bucketNames.contains(name)
        if (partOfExistingBuckets) {
          LOG.info("Skip initial bucket {}, it's part of the existing buckets.", name)
        }
        !partOfExistingBuckets
      }
      .forEach { name: String? ->
        bucketStore.createBucket(
          name!!,
          false,
          ObjectOwnership.BUCKET_OWNER_ENFORCED,
          mockRegion.id(),
          null,
          null
        )
        LOG.info("Creating initial bucket {}.", name)
      }

    return bucketStore
  }

  @Bean
  fun bucketNames(rootFolder: File): List<String> =
    try {
      rootFolder
        .toPath()
        .listDirectoryEntries()
        .mapNotNull {
          val meta = it.resolve(BUCKET_META_FILE).toFile()
          if (meta.exists()) {
            it.fileName.toString()
          } else {
            LOG.warn("Found bucket folder {} without {}", it, BUCKET_META_FILE)
            null
          }
        }
    } catch (e: IOException) {
      throw IllegalStateException(
        "Could not load buckets from data directory $rootFolder", e
      )
    }

  @Bean
  fun multipartStore(
    objectStore: ObjectStore,
    objectMapper: ObjectMapper
  ): MultipartStore = MultipartStore(objectStore, objectMapper)

  @Bean
  fun kmsKeyStore(
    properties: StoreProperties
  ): KmsKeyStore =
    KmsKeyStore(properties.validKmsKeys.ifEmpty { setOf() })

  @Bean
  fun rootFolder(
    properties: StoreProperties
  ): File {
    val rootPath = properties.root.takeIf { it.isNotEmpty() }
    val root: File =
      if (rootPath == null) {
        val baseTempDir = System.getProperty("java.io.tmpdir")?.let { File(it) }?.toPath()!!
        try {
          Files.createTempDirectory(baseTempDir, "s3mockFileStore").toFile()
        } catch (e: IOException) {
          throw IllegalStateException(
            "Root folder could not be created. Base temp dir: $baseTempDir", e
          )
        }
      } else {
        val dir = File(rootPath)
        if (dir.exists()) {
          LOG.info(
            "Using existing folder \"{}\" as root folder. Will retain files on exit: {}",
            dir.absolutePath, properties.retainFilesOnExit
          )
          // TODO: need to validate folder structure here?
        } else check(dir.mkdir()) {
          ("Root folder could not be created. Path: ${dir.absolutePath}")
        }
        dir
      }

    LOG.info(
      "Successfully created \"{}\" as root folder. Will retain files on exit: {}",
      root.absolutePath,
      properties.retainFilesOnExit
    )
    return root
  }

  @Bean
  fun storeCleaner(
    rootFolder: File,
    properties: StoreProperties
  ): StoreCleaner = StoreCleaner(rootFolder, properties.retainFilesOnExit)

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(StoreConfiguration::class.java)
    val S3_OBJECT_DATE_FORMAT: DateTimeFormatter = DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .withZone(ZoneId.of("UTC"))
  }
}
