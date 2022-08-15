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
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores buckets created in S3Mock.
 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-buckets-s3.html
 */
public class BucketStore {

  private static final Logger LOG = LoggerFactory.getLogger(BucketStore.class);

  private final File rootFolder;
  private final boolean retainFilesOnExit;
  private final DateTimeFormatter s3ObjectDateFormat;

  public BucketStore(File rootFolder, boolean retainFilesOnExit, List<String> initialBuckets,
      DateTimeFormatter s3ObjectDateFormat) {
    this.rootFolder = rootFolder;
    this.retainFilesOnExit = retainFilesOnExit;
    this.s3ObjectDateFormat = s3ObjectDateFormat;
    initialBuckets.forEach(this::createBucket);
  }

  /**
   * Lists all buckets managed by this BucketStore.
   *
   * @return List of all Buckets.
   */
  public List<Bucket> listBuckets() {
    final DirectoryStream.Filter<Path> filter = Files::isDirectory;

    return findBucketsByFilter(filter);
  }

  /**
   * Retrieves a bucket identified by its name.
   *
   * @param bucketName name of the bucket to be retrieved
   *
   * @return the Bucket or null if not found
   */
  public Bucket getBucket(final String bucketName) {
    final DirectoryStream.Filter<Path> filter =
        file -> (Files.isDirectory(file) && file.getFileName().endsWith(bucketName));

    final List<Bucket> buckets = findBucketsByFilter(filter);
    return buckets.size() > 0 ? buckets.get(0) : null;
  }


  /**
   * Searches for folders in the rootFolder that match the given {@link DirectoryStream.Filter}.
   *
   * @param filter the Filter to apply.
   *
   * @return List of found Folders.
   */
  private List<Bucket> findBucketsByFilter(final DirectoryStream.Filter<Path> filter) {
    final List<Bucket> buckets = new ArrayList<>();
    try (final DirectoryStream<Path> stream = Files
        .newDirectoryStream(rootFolder.toPath(), filter)) {
      for (final Path path : stream) {
        buckets.add(bucketFromPath(path));
      }
    } catch (final IOException e) {
      LOG.error("Could not Iterate over Bucket-Folders", e);
    }

    return buckets;
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
    final File newBucket = new File(rootFolder, bucketName);
    try {
      FileUtils.forceMkdir(newBucket);
    } catch (final IOException e) {
      throw new RuntimeException("Can't create bucket directory!", e);
    }
    if (!retainFilesOnExit) {
      newBucket.deleteOnExit();
    }
    return bucketFromPath(newBucket.toPath());
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
    final Bucket bucket = getBucket(bucketName);
    if (bucket != null) {
      FileUtils.deleteDirectory(bucket.getPath().toFile());
      return true;
    } else {
      return false;
    }
  }

  private Bucket bucketFromPath(final Path path) {
    Bucket result = null;
    final BasicFileAttributes attributes;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class);
      result =
          new Bucket(path,
              path.getFileName().toString(),
              s3ObjectDateFormat.format(attributes.creationTime().toInstant()));
    } catch (final IOException e) {
      LOG.error("File can not be read!", e);
    }

    return result;
  }

}
