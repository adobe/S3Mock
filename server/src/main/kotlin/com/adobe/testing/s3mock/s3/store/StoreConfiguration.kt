/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.s3.store

import com.adobe.testing.s3mock.s3.dto.ObjectOwnership
import com.adobe.testing.s3mock.s3.dto.S3_DATE_FORMAT
import com.adobe.testing.s3mock.s3.store.BucketStore.Companion.BUCKET_META_FILE
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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
    objectMapper: ObjectMapper,
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
    @Value($$"${com.adobe.testing.s3mock.store.region}") region: String?,
  ): BucketStore {
    val mockRegion = region ?: properties.region

    val bucketStore = BucketStore(rootFolder, S3_OBJECT_DATE_FORMAT, mockRegion, objectMapper)
    // load existing buckets first
    bucketStore.loadBuckets(bucketNames)

    // load initialBuckets if not part of existing buckets
    properties.initialBuckets
      .stream()
      .filter { it.isNotBlank() }
      .filter { name: String? ->
        val partOfExistingBuckets = bucketNames.contains(name)
        if (partOfExistingBuckets) {
          LOG.info("Skip initial bucket {}, it's part of the existing buckets.", name)
        }
        !partOfExistingBuckets
      }.forEach { name: String? ->
        bucketStore.createBucket(
          name!!,
          false,
          ObjectOwnership.BUCKET_OWNER_ENFORCED,
          mockRegion,
          null,
          null,
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
        "Could not load buckets from data directory $rootFolder",
        e,
      )
    }

  @Bean
  fun multipartStore(
    objectStore: ObjectStore,
    objectMapper: ObjectMapper,
  ): MultipartStore = MultipartStore(objectStore, objectMapper)

  @Bean
  fun kmsKeyStore(properties: StoreProperties): KmsKeyStore = KmsKeyStore(properties.validKmsKeys.ifEmpty { setOf() })

  @Bean
  fun rootFolder(properties: StoreProperties): File {
    val rootPath = properties.root.takeIf { it.isNotEmpty() }
    val root: File =
      if (rootPath == null) {
        val baseTempDir = System.getProperty("java.io.tmpdir")?.let { File(it) }?.toPath()!!
        try {
          Files.createTempDirectory(baseTempDir, "s3mockFileStore").toFile()
        } catch (e: IOException) {
          throw IllegalStateException(
            "Root folder could not be created. Base temp dir: $baseTempDir",
            e,
          )
        }
      } else {
        val dir = File(rootPath)
        if (!dir.exists()) {
          // mkdirs() creates any missing parent directories (mkdir() only creates the leaf and
          // fails for nested paths). It also returns false if another process already created
          // the directory in the meantime, so tolerate that race instead of failing spuriously.
          check(dir.mkdirs() || dir.isDirectory) {
            ("Root folder could not be created. Path: ${dir.absolutePath}")
          }
        }
        check(dir.isDirectory) {
          "Root folder \"${dir.absolutePath}\" exists but is not a directory."
        }
        dir
      }

    check(root.canWrite() && root.canExecute()) {
      "Root folder \"${root.absolutePath}\" is not writable/traversable by the current user " +
        "(\"${System.getProperty("user.name")}\"). Grant that user write and execute permission on the " +
        "directory (and its parents). If running the S3Mock Docker image: mount a named Docker volume at " +
        "/s3mockroot and set COM_ADOBE_TESTING_S3MOCK_STORE_ROOT=/s3mockroot - the image runs as the " +
        "non-root 'cnb' user and pre-creates that directory, so Docker makes a named volume mounted there " +
        "writable; a bind-mounted host directory instead keeps its host ownership and must be writable by " +
        "that user."
    }

    // Log both the configured value and its resolved absolute path: a *relative* store root
    // resolves against the process's current working directory, which is easy to misconfigure
    // (e.g. against a mounted volume) without any error - see
    // https://github.com/adobe/S3Mock/issues/3139.
    LOG.info(
      "Using \"{}\" (configured as \"{}\") as root folder. Will retain files on exit: {}",
      root.absolutePath,
      rootPath ?: "<default temp-dir>",
      properties.retainFilesOnExit,
    )
    return root
  }

  @Bean
  fun storeCleaner(
    rootFolder: File,
    properties: StoreProperties,
  ): StoreCleaner = StoreCleaner(rootFolder, properties.retainFilesOnExit)

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(StoreConfiguration::class.java)
    val S3_OBJECT_DATE_FORMAT: DateTimeFormatter =
      DateTimeFormatter
        .ofPattern(S3_DATE_FORMAT)
        .withZone(ZoneId.of("UTC"))
  }
}
