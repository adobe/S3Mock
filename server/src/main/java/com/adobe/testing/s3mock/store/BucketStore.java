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

import com.adobe.testing.s3mock.dto.Bucket;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores buckets and their metadata created in S3Mock.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-buckets-s3.html">API Reference</a>
 */
public class BucketStore {

  private static final Logger LOG = LoggerFactory.getLogger(BucketStore.class);
  private static final String BUCKET_META_FILE = "bucketMetadata";
  private final File rootFolder;
  private final boolean retainFilesOnExit;
  private final DateTimeFormatter s3ObjectDateFormat;
  private final ObjectMapper objectMapper;

  public BucketStore(File rootFolder, boolean retainFilesOnExit, List<String> initialBuckets,
      DateTimeFormatter s3ObjectDateFormat, ObjectMapper objectMapper) {
    this.rootFolder = rootFolder;
    this.retainFilesOnExit = retainFilesOnExit;
    this.s3ObjectDateFormat = s3ObjectDateFormat;
    this.objectMapper = objectMapper;
    initialBuckets.forEach(this::createBucket);
  }

  /**
   * Lists all buckets managed by this BucketStore.
   *
   * @return List of all Buckets.
   */
  public List<Bucket> listBuckets() {
    List<Bucket> buckets = new ArrayList<>();
    for (Path bucketPath : findBucketPaths()) {
      buckets.add(getBucket(bucketPath.getFileName().toString()));
    }
    return buckets;
  }

  /**
   * Retrieves a bucket identified by its name.
   *
   * @param bucketName name of the bucket to be retrieved
   *
   * @return the Bucket or null if not found
   */
  public Bucket getBucket(final String bucketName) {
    return bucketFromBucketMetadata(getBucketMetadata(bucketName));
  }

  /**
   * Retrieves BucketMetadata identified by its name.
   *
   * @param bucketName name of the bucket to be retrieved
   *
   * @return the BucketMetadata or null if not found
   */
  public BucketMetadata getBucketMetadata(final String bucketName) {
    Path metaFilePath = getMetaFilePath(bucketName);
    if (!metaFilePath.toFile().exists()) {
      return null;
    }

    BucketMetadata bucketMetadata;
    try {
      bucketMetadata = objectMapper.readValue(metaFilePath.toFile(), BucketMetadata.class);
    } catch (final IOException e) {
      throw new IllegalArgumentException("Could not read bucket metadata-file " + bucketName, e);
    }

    return bucketMetadata;
  }

  /**
   * Adds key to a bucket.
   *
   * @param key        the key to add
   * @param bucketName name of the bucket to be retrieved
   * @return UUID assigned to key
   */
  public UUID addToBucket(String key, String bucketName) {
    BucketMetadata bucketMetadata = getBucketMetadata(bucketName);
    UUID uuid = bucketMetadata.addKey(key);
    writeBucket(bucketMetadata);
    return uuid;
  }

  /**
   * Look up key in a bucket.
   *
   * @param key        the key to add
   * @param bucketName name of the bucket to be retrieved
   * @return UUID assigned to key
   */
  public UUID lookupKeyInBucket(String key, String bucketName) {
    BucketMetadata bucketMetadata = getBucketMetadata(bucketName);
    return bucketMetadata.getID(key);
  }

  /**
   * Look up keys by prefix in a bucket.
   *
   * @param prefix     the prefix to filter on
   * @param bucketName name of the bucket to be retrieved
   * @return List of UUIDs of keys matching the prefix
   */
  public List<UUID> lookupKeysInBucket(String prefix, String bucketName) {
    BucketMetadata bucketMetadata = getBucketMetadata(bucketName);
    String normalizedPrefix = prefix == null ? "" : prefix;
    return bucketMetadata.getObjects()
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().startsWith(normalizedPrefix))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  /**
   * Removes key from a bucket.
   *
   * @param key        the key to remove
   * @param bucketName name of the bucket to be retrieved
   * @return true if key existed and was removed
   */
  public boolean removeFromBucket(String key, String bucketName) {
    BucketMetadata bucketMetadata = getBucketMetadata(bucketName);
    boolean removed = bucketMetadata.removeKey(key);
    writeBucket(bucketMetadata);
    return removed;
  }

  /**
   * Searches for folders in the rootFolder.
   *
   * @return List of found Folders.
   */
  private List<Path> findBucketPaths() {
    final List<Path> bucketPaths = new ArrayList<>();
    try (final DirectoryStream<Path> stream = Files
        .newDirectoryStream(rootFolder.toPath(), Files::isDirectory)) {
      for (final Path path : stream) {
        bucketPaths.add(path);
      }
    } catch (final IOException e) {
      LOG.error("Could not Iterate over Bucket-Folders", e);
    }

    return bucketPaths;
  }

  /**
   * Creates a new bucket.
   *
   * @param bucketName name of the Bucket to be created.
   *
   * @return the newly created Bucket.
   *
   * @throws RuntimeException if the bucket cannot be created or the bucket already exists but is
   *     not a directory.
   */
  public Bucket createBucket(final String bucketName) {
    BucketMetadata bucketMetadata = getBucketMetadata(bucketName);
    if (bucketMetadata != null) {
      throw new IllegalStateException("Bucket already exists.");
    }
    final File bucketFolder = createBucketFolder(bucketName);

    BucketMetadata newBucketMetadata = new BucketMetadata();
    newBucketMetadata.setName(bucketName);
    newBucketMetadata.setCreationDate(s3ObjectDateFormat.format(LocalDateTime.now()));
    newBucketMetadata.setPath(bucketFolder.toPath());
    writeBucket(newBucketMetadata);
    return bucketFromBucketMetadata(newBucketMetadata);
  }

  public void writeBucket(BucketMetadata bucketMetadata) {
    try {
      File metaFile = getMetaFilePath(bucketMetadata.getName()).toFile();
      if (!retainFilesOnExit) {
        metaFile.deleteOnExit();
      }
      objectMapper.writeValue(metaFile, bucketMetadata);
    } catch (IOException e) {
      throw new IllegalStateException("Could not write bucket metadata-file", e);
    }
  }

  /**
   * Checks if the specified bucket exists. Amazon S3 buckets are named in a global namespace; use
   * this method to determine if a specified bucket name already exists, and therefore can't be used
   * to create a new bucket.
   *
   * @param bucketName Name of the bucket to check for existence
   *
   * @return true if Bucket exists
   */
  public Boolean doesBucketExist(final String bucketName) {
    return getBucket(bucketName) != null;
  }

  /**
   * Deletes a Bucket and all of its contents.
   * TODO: in S3, all objects within a bucket must be deleted before deleting a bucket!
   *
   * @param bucketName name of the bucket to be deleted.
   *
   * @return true if deletion succeeded.
   *
   * @throws IOException if bucket-file could not be accessed.
   */
  public boolean deleteBucket(final String bucketName) throws IOException {
    BucketMetadata bucketMetadata = getBucketMetadata(bucketName);
    if (bucketMetadata != null && bucketMetadata.getObjects().isEmpty()) {
      //TODO: this currently does not work, since we store objects below their prefixes, which are
      // not deleted when deleting the object, leaving empty directories in the S3Mock filesystem
      // should be: return Files.deleteIfExists(bucket.getPath())
      FileUtils.deleteDirectory(bucketMetadata.getPath().toFile());
      return true;
    } else {
      return false;
    }
  }

  private Bucket bucketFromBucketMetadata(BucketMetadata bucketMetadata) {
    if (bucketMetadata == null) {
      return null;
    }

    return new Bucket(bucketMetadata.getPath(),
        bucketMetadata.getName(),
        bucketMetadata.getCreationDate());
  }

  private Path getBucketFolderPath(String bucketName) {
    return Paths.get(rootFolder.getPath(), bucketName);
  }

  private File createBucketFolder(String bucketName) {
    final File bucketFolder = getBucketFolderPath(bucketName).toFile();
    try {
      FileUtils.forceMkdir(bucketFolder);
    } catch (final IOException e) {
      throw new IllegalStateException("Can't create bucket directory!", e);
    }
    if (!retainFilesOnExit) {
      bucketFolder.deleteOnExit();
    }
    return bucketFolder;
  }

  private Path getMetaFilePath(String bucketName) {
    return Paths.get(getBucketFolderPath(bucketName).toString(), BUCKET_META_FILE);
  }
}
