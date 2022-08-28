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

import static com.adobe.testing.s3mock.util.DigestUtil.hexDigest;
import static java.nio.file.Files.newOutputStream;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 Mock file store.
 */
public class FileStore {
  private static final String META_FILE = "metadata";
  private static final String DATA_FILE = "fileData";

  private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  private final boolean retainFilesOnExit;
  private final DateTimeFormatter s3ObjectDateFormat;

  private final ObjectMapper objectMapper;

  public FileStore(boolean retainFilesOnExit,
      DateTimeFormatter s3ObjectDateFormat, ObjectMapper objectMapper) {
    this.retainFilesOnExit = retainFilesOnExit;
    this.s3ObjectDateFormat = s3ObjectDateFormat;
    this.objectMapper = objectMapper;
  }

  /**
   * Stores an object inside a Bucket.
   *
   * @param bucket Bucket to store the object in.
   * @param key object key to be stored.
   * @param contentType The files Content Type.
   * @param contentEncoding The files Content Encoding.
   * @param dataStream The File as InputStream.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return {@link S3ObjectMetadata}.
   */
  public S3ObjectMetadata putS3Object(BucketMetadata bucket,
      UUID id,
      String key,
      String contentType,
      String contentEncoding,
      InputStream dataStream,
      boolean useV4ChunkedWithSigningFormat,
      Map<String, String> userMetadata,
      String encryption,
      String kmsKeyId,
      List<Tag> tags) {
    Instant now = Instant.now();
    boolean encrypted = isNotBlank(encryption) && isNotBlank(kmsKeyId);
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setId(id);
    s3ObjectMetadata.setName(key);
    s3ObjectMetadata.setContentType(contentType);
    s3ObjectMetadata.setContentEncoding(contentEncoding);
    s3ObjectMetadata.setUserMetadata(userMetadata);
    s3ObjectMetadata.setTags(tags);
    s3ObjectMetadata.setEncrypted(encrypted);
    s3ObjectMetadata.setKmsEncryption(encryption);
    s3ObjectMetadata.setKmsKeyId(kmsKeyId);
    s3ObjectMetadata.setModificationDate(s3ObjectDateFormat.format(now));
    s3ObjectMetadata.setLastModified(now.toEpochMilli());

    createObjectRootFolder(bucket, s3ObjectMetadata.getId());
    File dataFile =
        inputStreamToFile(wrapStream(dataStream, useV4ChunkedWithSigningFormat),
            getDataFilePath(bucket, s3ObjectMetadata.getId()));
    s3ObjectMetadata.setDataPath(dataFile.toPath());
    s3ObjectMetadata.setSize(Long.toString(dataFile.length()));
    s3ObjectMetadata.setEtag(hexDigest(kmsKeyId, dataFile));

    writeMetafile(bucket, s3ObjectMetadata);

    return s3ObjectMetadata;
  }

  InputStream wrapStream(InputStream dataStream, boolean useV4ChunkedWithSigningFormat) {
    InputStream inStream;
    if (useV4ChunkedWithSigningFormat) {
      inStream = new AwsChunkedDecodingInputStream(dataStream);
    } else {
      inStream = dataStream;
    }

    return inStream;
  }

  /**
   * Sets tags for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param id object ID to store tags for.
   * @param tags List of tag objects.
   */
  public void setObjectTags(BucketMetadata bucket, UUID id, List<Tag> tags) {
    S3ObjectMetadata s3ObjectMetadata = getS3Object(bucket, id);
    s3ObjectMetadata.setTags(tags);
    writeMetafile(bucket, s3ObjectMetadata);
  }

  /**
   * Sets user metadata for a given object.
   *
   * @param bucket Bucket where the object is stored in.
   * @param id object ID to store metadata for.
   * @param metadata Map of metadata.
   */
  public void setUserMetadata(BucketMetadata bucket, UUID id, Map<String, String> metadata) {
    S3ObjectMetadata s3ObjectMetadata = getS3Object(bucket, id);
    s3ObjectMetadata.setUserMetadata(metadata);
    writeMetafile(bucket, s3ObjectMetadata);
  }

  /**
   * Stores the content of an InputStream in a File.
   * Creates the File if it does not exist.
   *
   * @param inputStream the Stream to be saved.
   * @param filePath Path where the stream should be saved.
   *
   * @return the newly created File.
   */
  File inputStreamToFile(InputStream inputStream, Path filePath) {
    File targetFile = filePath.toFile();
    try {
      if (targetFile.createNewFile()) {
        if (!retainFilesOnExit) {
          targetFile.deleteOnExit();
        }
      }

      try (InputStream is = inputStream;
          OutputStream os = newOutputStream(targetFile.toPath())) {
        int read;
        byte[] bytes = new byte[1024];

        while ((read = is.read(bytes)) != -1) {
          os.write(bytes, 0, read);
        }
      }
    } catch (IOException e) {
      LOG.error("Wasn't able to store file on disk!", e);
      throw new IllegalStateException("Wasn't able to store file on disk!", e);
    }
    return targetFile;
  }

  /**
   * Retrieves S3ObjectMetadata for a UUID of a key from a bucket.
   *
   * @param bucket Bucket from which to retrieve the object.
   * @param uuid ID of the object key.
   *
   * @return S3ObjectMetadata or null if not found
   */
  public S3ObjectMetadata getS3Object(BucketMetadata bucket, UUID uuid) {
    S3ObjectMetadata theObject = null;

    Path metaPath = getMetaFilePath(bucket, uuid);

    if (Files.exists(metaPath)) {
      try {
        theObject = objectMapper.readValue(metaPath.toFile(), S3ObjectMetadata.class);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not read object metadata-file " + uuid, e);
      }
    }
    return theObject;
  }

  /**
   * Copies an object to another bucket and encrypted object.
   *
   * @param sourceBucket bucket to copy from.
   * @param sourceId source object ID.
   * @param destinationBucket destination bucket.
   * @param destinationId destination object ID.
   * @param destinationKey destination object key.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   * @param userMetadata User metadata to store for destination object
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   */
  public CopyObjectResult copyS3Object(BucketMetadata sourceBucket,
      UUID sourceId,
      BucketMetadata destinationBucket,
      UUID destinationId,
      String destinationKey,
      String encryption,
      String kmsKeyId,
      Map<String, String> userMetadata) {
    S3ObjectMetadata sourceObject = getS3Object(sourceBucket, sourceId);
    if (sourceObject == null) {
      return null;
    }

    S3ObjectMetadata copiedObject;
    try (InputStream inputStream = Files.newInputStream(sourceObject.getDataPath())) {
      copiedObject = putS3Object(destinationBucket,
          destinationId,
          destinationKey,
          sourceObject.getContentType(),
          sourceObject.getContentEncoding(),
          inputStream,
          false,
          userMetadata == null || userMetadata.isEmpty()
              ? sourceObject.getUserMetadata() : userMetadata,
          encryption,
          kmsKeyId,
          sourceObject.getTags());
    } catch (IOException e) {
      LOG.error("Wasn't able to store file on disk!", e);
      throw new IllegalStateException("Wasn't able to store file on disk!", e);
    }

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getEtag());
  }

  /**
   * If source and destination is the same, pretend we copied - S3 does the same.
   * This does not change the modificationDate.
   * Also, this would need to increment the version if/when we support versioning.
   */
  public CopyObjectResult pretendToCopyS3Object(BucketMetadata sourceBucket,
      UUID sourceId,
      Map<String, String> userMetadata) {
    S3ObjectMetadata sourceObject = getS3Object(sourceBucket, sourceId);
    if (sourceObject == null) {
      return null;
    }

    // overwrite metadata if necessary.
    setUserMetadata(sourceBucket, sourceId,
        userMetadata == null || userMetadata.isEmpty()
            ? sourceObject.getUserMetadata() : userMetadata);

    return new CopyObjectResult(sourceObject.getModificationDate(), sourceObject.getEtag());
  }

  /**
   * Removes an object key from a bucket.
   *
   * @param bucket bucket containing the object.
   * @param id object to be deleted.
   *
   * @return true if deletion succeeded.
   */
  public boolean deleteObject(BucketMetadata bucket, UUID id) {
    S3ObjectMetadata s3ObjectMetadata = getS3Object(bucket, id);
    if (s3ObjectMetadata != null) {
      try {
        FileUtils.deleteDirectory(s3ObjectMetadata.getDataPath().getParent().toFile());
      } catch (IOException e) {
        LOG.error("Wasn't able to delete directory.", e);
        throw new IllegalStateException("Wasn't able to delete directory.", e);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Creates the root folder in which to store data and meta file.
   *
   * @param bucket the Bucket containing the Object.
   *
   * @return The Folder to store the Object in.
   */
  private boolean createObjectRootFolder(BucketMetadata bucket, UUID id) {
    File objectRootFolder = getObjectFolderPath(bucket, id).toFile();
    return objectRootFolder.mkdirs();
  }

  private Path getObjectFolderPath(BucketMetadata bucket, UUID id) {
    return Paths.get(bucket.getPath().toString(), id.toString());
  }

  private Path getMetaFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), META_FILE);
  }

  Path getDataFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), DATA_FILE);
  }

  boolean writeMetafile(BucketMetadata bucket, S3ObjectMetadata s3ObjectMetadata) {
    try {
      File metaFile = getMetaFilePath(bucket, s3ObjectMetadata.getId()).toFile();
      if (!retainFilesOnExit) {
        metaFile.deleteOnExit();
      }
      objectMapper.writeValue(metaFile, s3ObjectMetadata);
      return true;
    } catch (IOException e) {
      LOG.error("Could not write object metadata-file.", e);
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }
}
