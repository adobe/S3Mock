/*
 *  Copyright 2017-2024 Adobe.
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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigest;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigestMultipart;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.newOutputStream;
import static org.apache.commons.io.FileUtils.openInputStream;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRange;

/**
 * Stores parts and their metadata created in S3Mock.
 */
public class MultipartStore extends StoreBase {
  private static final Logger LOG = LoggerFactory.getLogger(MultipartStore.class);
  private static final String PART_SUFFIX = ".part";
  private static final String MULTIPART_UPLOAD_META_FILE = "multipartMetadata.json";
  static final String MULTIPARTS_FOLDER = "multiparts";

  /**
   * This map stores one lock object per MultipartUpload ID.
   * Any method modifying the underlying file must acquire the lock object before the modification.
   */
  private final Map<UUID, Object> lockStore = new ConcurrentHashMap<>();
  private final boolean retainFilesOnExit;
  private final ObjectStore objectStore;
  private final ObjectMapper objectMapper;

  public MultipartStore(boolean retainFilesOnExit,
      ObjectStore objectStore,
      ObjectMapper objectMapper) {
    this.retainFilesOnExit = retainFilesOnExit;
    this.objectStore = objectStore;
    this.objectMapper = objectMapper;
  }

  /**
   * Prepares everything to store an object uploaded as multipart upload.
   *
   * @param bucket Bucket to upload object in
   * @param key object to upload
   * @param id ID of the object
   * @param contentType the content type
   * @param storeHeaders various headers to store
   * @param owner owner of the upload
   * @param initiator initiator of the upload
   * @param userMetadata custom metadata
   *
   * @return upload result
   */
  public MultipartUpload createMultipartUpload(BucketMetadata bucket,
                                               String key,
                                               UUID id,
                                               String contentType,
                                               Map<String, String> storeHeaders,
                                               Owner owner,
                                               Owner initiator,
                                               Map<String, String> userMetadata,
                                               Map<String, String> encryptionHeaders,
                                               StorageClass storageClass,
                                               String checksum,
                                               ChecksumAlgorithm checksumAlgorithm) {
    var uploadId = UUID.randomUUID().toString();
    if (!createPartsFolder(bucket, uploadId)) {
      LOG.error("Directories for storing multipart uploads couldn't be created. bucket={}, key={}, "
              + "id={}, uploadId={}", bucket, key, id, uploadId);
      throw new IllegalStateException(
          "Directories for storing multipart uploads couldn't be created.");
    }
    var upload = new MultipartUpload(key, uploadId, owner, initiator, storageClass, new Date());
    var multipartUploadInfo = new MultipartUploadInfo(upload,
        contentType,
        userMetadata,
        storeHeaders,
        encryptionHeaders,
        bucket.name(),
        storageClass,
        checksum,
        checksumAlgorithm);
    lockStore.putIfAbsent(UUID.fromString(uploadId), new Object());
    writeMetafile(bucket, multipartUploadInfo);

    return upload;
  }

  /**
   * Lists all not-yet completed parts of multipart uploads in a bucket.
   *
   * @param bucketMetadata the bucket to use as a filter
   * @param prefix the prefix use as a filter
   *
   * @return the list of not-yet completed multipart uploads.
   */
  public List<MultipartUpload> listMultipartUploads(BucketMetadata bucketMetadata, String prefix) {
    Path multipartsFolder = getMultipartsFolder(bucketMetadata);
    if (!multipartsFolder.toFile().exists()) {
      return Collections.emptyList();
    }
    try (var paths = Files.newDirectoryStream(multipartsFolder)) {
      return Streams.of(paths)
          .map(path -> {
                var fileName = path.getFileName().toString();
                return getUploadMetadata(bucketMetadata, fileName).upload();
              }
          )
          .filter(multipartUpload -> isBlank(prefix) || multipartUpload.key().startsWith(prefix))
          .toList();
    } catch (IOException e) {
      throw new IllegalStateException("Could not load buckets from data directory ", e);
    }
  }

  public MultipartUploadInfo getMultipartUploadInfo(BucketMetadata bucketMetadata,
                                                    String uploadId) {
    return getUploadMetadata(bucketMetadata, uploadId);
  }

  /**
   * Get MultipartUpload, if it was not completed.
   * @param uploadId id of the upload
   *
   * @return the multipart upload, if it exists, throws IllegalArgumentException otherwise.
   */
  public MultipartUpload getMultipartUpload(BucketMetadata bucketMetadata, String uploadId) {
    var uploadMetadata = getUploadMetadata(bucketMetadata, uploadId);
    if (uploadMetadata != null) {
      return uploadMetadata.upload();
    } else {
      throw new IllegalArgumentException("No MultipartUpload found with uploadId: " + uploadId);
    }
  }

  /**
   * Aborts the upload.
   *
   * @param bucket to which was uploaded
   * @param id of the object
   * @param uploadId of the upload
   */
  public void abortMultipartUpload(BucketMetadata bucket, UUID id, String uploadId) {
    var multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId);
    if (multipartUploadInfo != null) {
      synchronized (lockStore.get(UUID.fromString(uploadId))) {
        try {
          FileUtils.deleteDirectory(getPartsFolder(bucket, uploadId).toFile());
        } catch (IOException e) {
          throw new IllegalStateException("Could not delete multipart-directory " + uploadId, e);
        }
        lockStore.remove(UUID.fromString(uploadId));
      }
    }
  }

  /**
   * Uploads a part of a multipart upload.
   *
   * @param bucket                    in which to upload
   * @param id                      of the object to upload
   * @param uploadId                      id of the upload
   * @param partNumber                    number of the part to store
   * @param path                     file data to be stored
   *
   * @return the md5 digest of this part
   */
  public String putPart(BucketMetadata bucket,
      UUID id,
      String uploadId,
      String partNumber,
      Path path,
      Map<String, String> encryptionHeaders) {
    var file = inputPathToFile(path,
        getPartPath(bucket, uploadId, partNumber),
        retainFilesOnExit);

    return hexDigest(encryptionHeaders.get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID), file);
  }

  /**
   * Completes a Multipart Upload for the given ID.
   *
   * @param bucket in which to upload.
   * @param key of the object to upload.
   * @param id id of the object
   * @param uploadId id of the upload.
   * @param parts to concatenate.
   *
   * @return etag of the uploaded file.
   */
  public String completeMultipartUpload(BucketMetadata bucket, String key, UUID id,
      String uploadId, List<CompletedPart> parts, Map<String, String> encryptionHeaders) {
    var uploadInfo = getMultipartUploadInfo(bucket, uploadId);
    if (uploadInfo == null) {
      throw new IllegalArgumentException("Unknown upload " + uploadId);
    }
    var partFolder = getPartsFolder(bucket, uploadId);
    var partsPaths =
        parts
            .stream()
            .map(part ->
                Paths.get(partFolder.toString(), part.partNumber() + PART_SUFFIX)
            )
            .toList();
    Path tempFile = null;
    try (var inputStream = toInputStream(partsPaths)) {
      tempFile = Files.createTempFile("completeMultipartUpload", "");
      inputStream.transferTo(Files.newOutputStream(tempFile));
      var checksumFor = checksumFor(tempFile, uploadInfo);
      var etag = hexDigestMultipart(partsPaths);
      objectStore.storeS3ObjectMetadata(bucket,
          id,
          key,
          uploadInfo.contentType(),
          uploadInfo.storeHeaders(),
          tempFile,
          uploadInfo.userMetadata(),
          encryptionHeaders,
          etag,
          Collections.emptyList(), //TODO: no tags for multi part uploads?
          uploadInfo.checksumAlgorithm(),
          checksumFor,
          uploadInfo.upload().owner(),
          uploadInfo.storageClass()
      );
      FileUtils.deleteDirectory(partFolder.toFile());
      return etag;
    } catch (IOException e) {
      throw new IllegalStateException(String.format(
          "Error finishing multipart upload bucket=%s, key=%s, id=%s, uploadId=%s",
          bucket, key, id, uploadId), e);
    } finally {
      if (tempFile != null) {
        tempFile.toFile().delete();
      }
    }
  }

  private String checksumFor(Path path, MultipartUploadInfo uploadInfo) {
    if (uploadInfo.checksumAlgorithm() != null) {
      return DigestUtil.checksumFor(path, uploadInfo.checksumAlgorithm().toAlgorithm());
    }
    return null;
  }

  /**
   * Get all multipart upload parts.
   * @param bucket name of the bucket
   * @param id object ID
   * @param uploadId upload identifier
   * @return List of Parts
   */
  public List<Part> getMultipartUploadParts(BucketMetadata bucket, UUID id, String uploadId) {
    var partsPath = getPartsFolder(bucket, uploadId);
    try (var directoryStream = newDirectoryStream(partsPath,
            path -> path.getFileName().toString().endsWith(PART_SUFFIX))) {
      return StreamSupport.stream(directoryStream.spliterator(), false)
          .map(path -> {
            String name = path.getFileName().toString();
            String prefix = name.substring(0, name.indexOf('.'));
            int partNumber = Integer.parseInt(prefix);
            String partMd5 = hexDigest(path.toFile());
            Date lastModified = new Date(path.toFile().lastModified());

            return new Part(partNumber, partMd5, lastModified, path.toFile().length());
          })
          .sorted(Comparator.comparing(Part::partNumber))
          .toList();
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Could not read all parts. "
          + "bucket=%s, id=%s, uploadId=%s", bucket, id, uploadId), e);
    }
  }

  /**
   * Copies the range, define by from/to, from the S3 Object, identified by the given key to given
   * destination into the given bucket.
   *
   * @param bucket The source Bucket.
   * @param id Identifies the S3 Object.
   * @param copyRange Byte range to copy. Optional.
   * @param partNumber The part to copy.
   * @param destinationBucket The Bucket the target object (will) reside in.
   * @param destinationId The target object ID.
   * @param uploadId id of the upload.
   *
   * @return etag of the uploaded file.
   */
  public String copyPart(BucketMetadata bucket,
      UUID id,
      HttpRange copyRange,
      String partNumber,
      BucketMetadata destinationBucket,
      UUID destinationId,
      String uploadId,
      Map<String, String> encryptionHeaders) {

    verifyMultipartUploadPreparation(destinationBucket, destinationId, uploadId);

    return copyPartToFile(bucket, id, copyRange,
        createPartFile(destinationBucket, destinationId, uploadId, partNumber));
  }

  /**
   * Returns an InputStream containing InputStreams from each path element.
   * @param paths the paths to read
   * @return an InputStream containing all data.
   */
  private static InputStream toInputStream(List<Path> paths) {
    var result = new ArrayList<InputStream>();
    for (var path: paths) {
      try {
        result.add(Files.newInputStream(path));
      } catch (IOException e) {
        throw new IllegalStateException("Can't access path " + path, e);
      }
    }
    return new SequenceInputStream(Collections.enumeration(result));
  }

  private String copyPartToFile(BucketMetadata bucket,
      UUID id,
      HttpRange copyRange,
      File partFile) {
    var from = 0L;
    var s3ObjectMetadata = objectStore.getS3ObjectMetadata(bucket, id);
    var len = s3ObjectMetadata.dataPath().toFile().length();
    if (copyRange != null) {
      from = copyRange.getRangeStart(len);
      len = copyRange.getRangeEnd(len) - copyRange.getRangeStart(len) + 1;
    }

    try (var sourceStream = openInputStream(s3ObjectMetadata.dataPath().toFile());
        var targetStream = newOutputStream(partFile.toPath())) {
      var skip = sourceStream.skip(from);
      if (skip == from) {
        IOUtils.copy(BoundedInputStream
            .builder()
            .setInputStream(sourceStream)
            .setMaxCount(len)
            .get(),
            targetStream
        );
      } else {
        throw new IllegalStateException("Could not skip exact byte range");
      }
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Could not copy object. "
          + "bucket=%s, id=%s, range=%s, partFile=%s", bucket, id, copyRange, partFile), e);
    }
    return hexDigest(partFile);
  }

  private File createPartFile(BucketMetadata bucket,
      UUID id,
      String uploadId,
      String partNumber) {
    if (id == null) {
      return null;
    }
    var partFile = getPartPath(
        bucket,
        uploadId,
        partNumber).toFile();

    try {
      if (!partFile.exists() && !partFile.createNewFile()) {
        throw new IllegalStateException(String.format("Could not create buffer file. "
            + "bucket=%s, id=%s, uploadId=%s, partNumber=%s", bucket, id, uploadId, partNumber));
      }
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Could not create buffer file. "
          + "bucket=%s, id=%s, uploadId=%s, partNumber=%s", bucket, id, uploadId, partNumber), e);
    }
    return partFile;
  }

  private void verifyMultipartUploadPreparation(BucketMetadata bucket, UUID id, String uploadId) {
    Path partsFolder = null;
    var multipartUploadInfo = getMultipartUploadInfo(bucket, uploadId);
    if (id != null) {
      partsFolder = getPartsFolder(bucket, uploadId);
    }

    if (multipartUploadInfo == null
        || partsFolder == null
        || !partsFolder.toFile().exists()
        || !partsFolder.toFile().isDirectory()) {
      throw new IllegalStateException(String.format(
          "Multipart Request was not prepared. bucket=%s, id=%s, uploadId=%s, partsFolder=%s",
          bucket, id, uploadId, partsFolder));
    }
  }

  private boolean createPartsFolder(BucketMetadata bucket, String uploadId) {
    var partsFolder = getPartsFolder(bucket, uploadId).toFile();
    var created = partsFolder.mkdirs();
    if (created && !retainFilesOnExit) {
      partsFolder.deleteOnExit();
    }
    return created;
  }

  private Path getMultipartsFolder(BucketMetadata bucket) {
    return Paths.get(bucket.path().toString(), MULTIPARTS_FOLDER);
  }

  private Path getPartsFolder(BucketMetadata bucket, String uploadId) {
    return getMultipartsFolder(bucket).resolve(uploadId);
  }

  private Path getPartPath(BucketMetadata bucket, String uploadId, String partNumber) {
    return getPartsFolder(bucket, uploadId).resolve(partNumber + PART_SUFFIX);
  }

  private Path getUploadMetadataPath(BucketMetadata bucket, String uploadId) {
    return getPartsFolder(bucket, uploadId).resolve(MULTIPART_UPLOAD_META_FILE);
  }

  public MultipartUploadInfo getUploadMetadata(BucketMetadata bucket, String uploadId) {
    var metaPath = getUploadMetadataPath(bucket, uploadId);

    if (Files.exists(metaPath)) {
      synchronized (lockStore.get(UUID.fromString(uploadId))) {
        try {
          return objectMapper.readValue(metaPath.toFile(), MultipartUploadInfo.class);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read upload metadata-file " + uploadId, e);
        }
      }
    }
    return null;
  }

  private void writeMetafile(BucketMetadata bucket, MultipartUploadInfo uploadInfo) {
    var uploadId = uploadInfo.upload().uploadId();
    try {
      synchronized (lockStore.get(UUID.fromString(uploadId))) {
        var metaFile = getUploadMetadataPath(bucket, uploadId).toFile();
        if (!retainFilesOnExit) {
          metaFile.deleteOnExit();
        }
        objectMapper.writeValue(metaFile, uploadInfo);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write upload metadata-file " + uploadId, e);
    }
  }

  public MultipartUploadInfo getUploadMetadata(BucketMetadata bucket, UUID id, String uploadId) {
    var metaPath = getUploadMetadataPath(bucket, id, uploadId);

    if (Files.exists(metaPath)) {
      synchronized (lockStore.get(id)) {
        try {
          return objectMapper.readValue(metaPath.toFile(), MultipartUploadInfo.class);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read upload metadata-file " + id, e);
        }
      }
    }
    return null;
  }

  private void writeMetafile(BucketMetadata bucket, UUID id, MultipartUploadInfo uploadInfo) {
    var uploadId = uploadInfo.upload().uploadId();
    try {
      synchronized (lockStore.get(id)) {
        var metaFile = getUploadMetadataPath(bucket, id, uploadId).toFile();
        if (!retainFilesOnExit) {
          metaFile.deleteOnExit();
        }
        objectMapper.writeValue(metaFile, uploadInfo);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write upload metadata-file " + id, e);
    }
  }
}
