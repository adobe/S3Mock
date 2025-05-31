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

import com.adobe.testing.s3mock.dto.BucketInfo;
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.LocationInfo;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.dto.ObjectLockEnabled;
import com.adobe.testing.s3mock.dto.ObjectOwnership;
import com.adobe.testing.s3mock.dto.VersioningConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores buckets and their metadata created in S3Mock.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-buckets-s3.html">API Reference</a>
 */
public class BucketStore {

  private static final Logger LOG = LoggerFactory.getLogger(BucketStore.class);
  static final String BUCKET_META_FILE = "bucketMetadata.json";
  /**
   * This map stores one lock object per Bucket name.
   * Any method modifying the underlying file must aquire the lock object before the modification.
   */
  private final Map<String, Object> lockStore = new ConcurrentHashMap<>();
  private final File rootFolder;
  private final DateTimeFormatter s3ObjectDateFormat;
  private final ObjectMapper objectMapper;

  public BucketStore(File rootFolder,
      DateTimeFormatter s3ObjectDateFormat,
      ObjectMapper objectMapper) {
    this.rootFolder = rootFolder;
    this.s3ObjectDateFormat = s3ObjectDateFormat;
    this.objectMapper = objectMapper;
  }

  public List<BucketMetadata> listBuckets() {
    return findBucketPaths()
        .stream()
        .map(path -> path.getFileName().toString())
        .map(this::getBucketMetadata)
        .toList();
  }

  public BucketMetadata getBucketMetadata(String bucketName) {
    try {
      var metaFilePath = getMetaFilePath(bucketName);
      if (!metaFilePath.toFile().exists()) {
        return null;
      }
      synchronized (lockStore.get(bucketName)) {
        return objectMapper.readValue(metaFilePath.toFile(), BucketMetadata.class);
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Could not read bucket metadata-file " + bucketName, e);
    }
  }

  public synchronized UUID addKeyToBucket(String key, String bucketName) {
    synchronized (lockStore.get(bucketName)) {
      var bucketMetadata = getBucketMetadata(bucketName);
      var uuid = bucketMetadata.addKey(key);
      writeToDisk(bucketMetadata);
      return uuid;
    }
  }

  public List<UUID> lookupKeysInBucket(String prefix, String bucketName) {
    var bucketMetadata = getBucketMetadata(bucketName);
    var normalizedPrefix = prefix == null ? "" : prefix;
    synchronized (lockStore.get(bucketName)) {
      return bucketMetadata.objects()
          .entrySet()
          .stream()
          .filter(entry -> entry.getKey().startsWith(normalizedPrefix))
          .map(Map.Entry::getValue)
          .toList();
    }
  }

  public synchronized boolean removeFromBucket(String key, String bucketName) {
    synchronized (lockStore.get(bucketName)) {
      var bucketMetadata = getBucketMetadata(bucketName);
      var removed = bucketMetadata.removeKey(key);
      writeToDisk(bucketMetadata);
      return removed;
    }
  }

  private List<Path> findBucketPaths() {
    var bucketPaths = new ArrayList<Path>();
    try (var stream = Files.newDirectoryStream(rootFolder.toPath(), Files::isDirectory)) {
      for (var path : stream) {
        bucketPaths.add(path);
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Could not Iterate over Bucket-Folders.", e);
    }

    return bucketPaths;
  }

  public BucketMetadata createBucket(String bucketName,
      boolean objectLockEnabled,
      ObjectOwnership objectOwnership,
      String bucketRegion,
      BucketInfo bucketInfo,
      LocationInfo locationInfo) {
    var bucketMetadata = getBucketMetadata(bucketName);
    if (bucketMetadata != null) {
      throw new IllegalStateException("Bucket already exists.");
    }
    lockStore.putIfAbsent(bucketName, new Object());
    synchronized (lockStore.get(bucketName)) {
      var bucketFolder = createBucketFolder(bucketName);

      var newBucketMetadata = new BucketMetadata(
          bucketName,
          s3ObjectDateFormat.format(LocalDateTime.now()),
          new VersioningConfiguration(null, null, null),
          objectLockEnabled
              ? new ObjectLockConfiguration(ObjectLockEnabled.ENABLED, null) : null,
          null,
          objectOwnership,
          bucketFolder.toPath(),
          bucketRegion,
          bucketInfo,
          locationInfo
      );
      writeToDisk(newBucketMetadata);
      return newBucketMetadata;
    }
  }

  /**
   * Checks if the specified bucket exists. Amazon S3 buckets are named in a global namespace; use
   * this method to determine if a specified bucket name already exists, and therefore can't be used
   * to create a new bucket.
   */
  public boolean doesBucketExist(String bucketName) {
    return getBucketMetadata(bucketName) != null;
  }

  public boolean isObjectLockEnabled(String bucketName) {
    var objectLockConfiguration = getBucketMetadata(bucketName).objectLockConfiguration();
    if (objectLockConfiguration != null) {
      return ObjectLockEnabled.ENABLED == objectLockConfiguration.objectLockEnabled();
    }
    return false;
  }

  public void storeObjectLockConfiguration(BucketMetadata metadata,
      ObjectLockConfiguration configuration) {
    synchronized (lockStore.get(metadata.name())) {
      writeToDisk(metadata.withObjectLockConfiguration(configuration));
    }
  }

  public void storeVersioningConfiguration(BucketMetadata metadata,
      VersioningConfiguration configuration) {
    synchronized (lockStore.get(metadata.name())) {
      writeToDisk(metadata.withVersioningConfiguration(configuration));
    }
  }

  public void storeBucketLifecycleConfiguration(BucketMetadata metadata,
      BucketLifecycleConfiguration configuration) {
    synchronized (lockStore.get(metadata.name())) {
      writeToDisk(metadata.withBucketLifecycleConfiguration(configuration));
    }
  }

  public boolean isBucketEmpty(String bucketName) {
    var bucketMetadata = getBucketMetadata(bucketName);
    if (bucketMetadata != null) {
      return bucketMetadata.objects().isEmpty();
    } else {
      throw new IllegalStateException("Requested Bucket does not exist: " + bucketName);
    }
  }

  public boolean deleteBucket(String bucketName) {
    try {
      synchronized (lockStore.get(bucketName)) {
        var bucketMetadata = getBucketMetadata(bucketName);
        if (bucketMetadata != null && bucketMetadata.objects().isEmpty()) {
          FileUtils.deleteDirectory(bucketMetadata.path().toFile());
          lockStore.remove(bucketName);
          return true;
        } else {
          return false;
        }
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Can't delete bucket directory!", e);
    }
  }

  /**
   * Used to load metadata for all buckets when S3Mock starts.
   */
  List<UUID> loadBuckets(List<String> bucketNames) {
    var objectIds = new ArrayList<UUID>();
    for (String bucketName : bucketNames) {
      LOG.info("Loading existing bucket {}.", bucketName);
      lockStore.putIfAbsent(bucketName, new Object());
      synchronized (lockStore.get(bucketName)) {
        var bucketMetadata = getBucketMetadata(bucketName);
        var objects = bucketMetadata.objects();
        for (Map.Entry<String, UUID> objectEntry : objects.entrySet()) {
          objectIds.add(objectEntry.getValue());
          LOG.info("Loading existing bucket {} key {}", bucketName, objectEntry.getKey());
        }
      }
    }
    return objectIds;
  }

  private void writeToDisk(BucketMetadata bucketMetadata) {
    try {
      var metaFile = getMetaFilePath(bucketMetadata.name()).toFile();
      synchronized (lockStore.get(bucketMetadata.name())) {
        objectMapper.writeValue(metaFile, bucketMetadata);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write bucket metadata-file", e);
    }
  }

  private Path getBucketFolderPath(String bucketName) {
    return Paths.get(rootFolder.getPath(), bucketName);
  }

  private File createBucketFolder(String bucketName) {
    try {
      var bucketFolder = getBucketFolderPath(bucketName).toFile();
      FileUtils.forceMkdir(bucketFolder);
      return bucketFolder;
    } catch (final IOException e) {
      throw new IllegalStateException("Can't create bucket directory!", e);
    }
  }

  private Path getMetaFilePath(String bucketName) {
    return Paths.get(getBucketFolderPath(bucketName).toString(), BUCKET_META_FILE);
  }
}
