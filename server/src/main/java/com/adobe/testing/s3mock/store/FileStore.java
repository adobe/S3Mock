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
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigestMultipart;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.newOutputStream;
import static org.apache.commons.io.FileUtils.openInputStream;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 Mock file store.
 */
public class FileStore {
  private static final String META_FILE = "metadata";
  private static final String DATA_FILE = "fileData";
  private static final String PART_SUFFIX = ".part";

  private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  private final boolean retainFilesOnExit;
  private final DateTimeFormatter s3ObjectDateFormat;

  private final ObjectMapper objectMapper;

  private final Map<String, MultipartUploadInfo> uploadIdToInfo = new ConcurrentHashMap<>();

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

  private InputStream wrapStream(InputStream dataStream, boolean useV4ChunkedWithSigningFormat) {
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
  private File inputStreamToFile(InputStream inputStream, Path filePath) {
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
   * Prepares everything to store an object uploaded as multipart upload.
   *
   * @param bucket Bucket to upload object in
   * @param key object to upload
   * @param contentType the content type
   * @param contentEncoding the content encoding
   * @param uploadId id of the upload
   * @param owner owner of the upload
   * @param initiator initiator of the upload
   * @param userMetadata custom metadata
   *
   * @return upload result
   */
  public MultipartUpload prepareMultipartUpload(BucketMetadata bucket, String key, UUID uuid,
      String contentType, String contentEncoding, String uploadId,
      Owner owner, Owner initiator, Map<String, String> userMetadata) {
    if (!createPartsFolder(bucket, uuid, uploadId)) {
      LOG.error("Directories for storing multipart uploads couldn't be created.");
      throw new IllegalStateException(
          "Directories for storing multipart uploads couldn't be created.");
    }
    MultipartUpload upload =
        new MultipartUpload(key, uploadId, owner, initiator, new Date());
    uploadIdToInfo.put(uploadId, new MultipartUploadInfo(upload,
        contentType, contentEncoding, userMetadata, bucket.getName()));

    return upload;
  }

  /**
   * Lists all not-yet completed parts of multipart uploads in a bucket.
   *
   * @param bucket the bucket to use as a filter
   * @param prefix the prefix use as a filter
   *
   * @return the list of not-yet completed multipart uploads.
   */
  public List<MultipartUpload> listMultipartUploads(String bucket, String prefix) {
    return uploadIdToInfo.values()
        .stream()
        .filter(info -> bucket == null || bucket.equals(info.bucket))
        .filter(info -> isBlank(prefix) || info.upload.getKey().startsWith(prefix))
        .map(info -> info.upload)
        .collect(Collectors.toList());
  }

  /**
   * Returns the not-yet multipart upload, if it exists, throws IllegalArgumentException otherwise.
   */
  public MultipartUpload getMultipartUpload(String uploadId) {
    return uploadIdToInfo.values()
        .stream()
        .filter(info -> uploadId.equals(info.upload.getUploadId()))
        .map(info -> info.upload)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No MultipartUpload found with uploadId"));
  }

  /**
   * Aborts the upload.
   *
   * @param bucket to which was uploaded
   * @param uploadId of the upload
   */
  public void abortMultipartUpload(BucketMetadata bucket, UUID uuid, String uploadId) {
    synchronizedUpload(uploadId, uploadInfo -> {

      try {
        File partFolder = getPartsFolderPath(bucket, uuid, uploadId).toFile();
        FileUtils.deleteDirectory(partFolder);

        File dataFile = getDataFilePath(bucket, uuid).toFile();
        FileUtils.deleteQuietly(dataFile);

        uploadIdToInfo.remove(uploadId);
        return null;
      } catch (IOException e) {
        LOG.error("Could not delete multipart upload tmp data.", e);
        throw new IllegalStateException("Could not delete multipart upload tmp data.", e);
      }
    });
  }

  /**
   * Uploads a part of a multipart upload.
   *
   * @param bucket                    in which to upload
   * @param uuid                      of the object to upload
   * @param uploadId                      id of the upload
   * @param partNumber                    number of the part to store
   * @param inputStream                   file data to be stored
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param encryption                    whether to use encryption, and possibly which type
   * @param kmsKeyId                      the ID of the KMS key to use.
   *
   * @return the md5 digest of this part
   */
  public String putPart(BucketMetadata bucket,
      UUID uuid,
      String uploadId,
      String partNumber,
      InputStream inputStream,
      boolean useV4ChunkedWithSigningFormat,
      String encryption,
      String kmsKeyId) {
    File file = inputStreamToFile(wrapStream(inputStream, useV4ChunkedWithSigningFormat),
        getPartPath(bucket, uuid, uploadId, partNumber));

    return hexDigest(kmsKeyId, file);
  }

  /**
   * Completes a Multipart Upload for the given ID.
   *
   * @param bucket in which to upload.
   * @param key of the file to upload.
   * @param uploadId id of the upload.
   * @param parts to concatenate.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return etag of the uploaded file.
   */
  public String completeMultipartUpload(BucketMetadata bucket, String key, UUID uuid,
      String uploadId, List<CompletedPart> parts, String encryption, String kmsKeyId) {
    return synchronizedUpload(uploadId, uploadInfo -> {
      S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
      s3ObjectMetadata.setId(uuid);
      s3ObjectMetadata.setName(key);

      s3ObjectMetadata.setEncrypted(encryption != null || kmsKeyId != null);
      s3ObjectMetadata.setKmsEncryption(encryption);
      s3ObjectMetadata.setKmsKeyId(kmsKeyId);

      Path partFolder = getPartsFolderPath(bucket, uuid, uploadId);
      Path entireFile = getDataFilePath(bucket, uuid);

      List<Path> partsPaths =
          parts
              .stream()
              .map(part ->
                  Paths.get(partFolder.toString(), part.getPartNumber() + PART_SUFFIX)
              )
              .collect(Collectors.toList());

      long size = writeEntireFile(entireFile, partsPaths);
      s3ObjectMetadata.setDataPath(entireFile);
      try {
        Instant now = Instant.now();
        s3ObjectMetadata.setModificationDate(s3ObjectDateFormat.format(now));
        s3ObjectMetadata.setLastModified(now.toEpochMilli());
        s3ObjectMetadata.setEtag(hexDigestMultipart(partsPaths));
        s3ObjectMetadata.setSize(Long.toString(size));
        s3ObjectMetadata.setContentType(uploadInfo.contentType);
        s3ObjectMetadata.setContentEncoding(uploadInfo.contentEncoding);
        s3ObjectMetadata.setUserMetadata(uploadInfo.userMetadata);

        uploadIdToInfo.remove(uploadId);
        FileUtils.deleteDirectory(partFolder.toFile());
      } catch (IOException e) {
        LOG.error("Error finishing multipart upload", e);
        throw new IllegalStateException("Error finishing multipart upload.", e);
      }

      writeMetafile(bucket, s3ObjectMetadata);

      return s3ObjectMetadata.getEtag();
    });
  }

  /**
   * Get all multipart upload parts.
   * @param bucket name of the bucket
   * @param uuid object ID
   * @param uploadId upload identifier
   * @return List of Parts
   */
  public List<Part> getMultipartUploadParts(BucketMetadata bucket, UUID uuid, String uploadId) {
    Path partsPath = getPartsFolderPath(bucket, uuid, uploadId);
    try (DirectoryStream<Path> directoryStream =
        newDirectoryStream(partsPath,
            path -> path.getFileName().toString().endsWith(PART_SUFFIX))) {
      return StreamSupport.stream(directoryStream.spliterator(), false)
          .map(path -> {
            String name = path.getFileName().toString();
            String prefix = name.substring(0, name.indexOf('.'));
            int partNumber = Integer.parseInt(prefix);
            String partMd5 = hexDigest(path.toFile());
            Date lastModified = new Date(path.toFile().lastModified());

            Part part = new Part();
            part.setLastModified(lastModified);
            part.setETag(partMd5);
            part.setPartNumber((partNumber));
            part.setSize(path.toFile().length());
            return part;
          })
          .sorted(Comparator.comparing(CompletedPart::getPartNumber))
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.error("Could not read all parts.", e);
      throw new IllegalStateException("Could not read all parts.", e);
    }
  }

  /**
   * Write contents of all parts into an entire file.
   *
   * @param partsPaths the paths of all parts.
   *
   * @return The size of the entire file after processing.
   *
   * @throws IllegalStateException if accessing / reading / writing of files is not possible.
   */
  private long writeEntireFile(Path entireFile, List<Path> partsPaths) {
    try (OutputStream targetStream = newOutputStream(entireFile)) {
      long size = 0;
      for (Path partPath : partsPaths) {
        size += Files.copy(partPath, targetStream);
      }
      return size;
    } catch (IOException e) {
      LOG.error("Error writing entire file {}", entireFile, e);
      throw new IllegalStateException("Error writing entire file " + entireFile, e);
    }
  }

  /**
   * Synchronize access on the upload, to handle concurrent abortion/completion.
   */
  private <T> T synchronizedUpload(String uploadId,
      Function<MultipartUploadInfo, T> callback) {

    MultipartUploadInfo uploadInfo = uploadIdToInfo.get(uploadId);
    if (uploadInfo == null) {
      throw new IllegalArgumentException("Unknown upload " + uploadId);
    }

    // we assume that an uploadId -> uploadInfo is only registered once and not modified in between,
    // therefore we can synchronize on the uploadInfo instance
    synchronized (uploadInfo) {
      // check if the upload was aborted or completed in the meantime
      if (!uploadIdToInfo.containsKey(uploadId)) {
        LOG.error("Upload {} was aborted or completed concurrently", uploadId);
        throw new IllegalStateException(
            "Upload " + uploadId + " was aborted or completed concurrently");
      }
      return callback.apply(uploadInfo);
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
      Range copyRange,
      String partNumber,
      BucketMetadata destinationBucket,
      UUID destinationId,
      String uploadId) {

    verifyMultipartUploadPreparation(destinationBucket, destinationId, uploadId);

    File targetPartFile = ensurePartFile(partNumber, destinationBucket, destinationId, uploadId);

    return copyPart(bucket, id, copyRange, targetPartFile);
  }

  private String copyPart(BucketMetadata bucket,
      UUID id,
      Range copyRange,
      File partFile) {
    long from = 0;
    S3ObjectMetadata s3ObjectMetadata = getS3Object(bucket, id);
    long len = s3ObjectMetadata.getDataPath().toFile().length();
    if (copyRange != null) {
      from = copyRange.getStart();
      len = copyRange.getEnd() - copyRange.getStart() + 1;
    }

    try (InputStream sourceStream = openInputStream(s3ObjectMetadata.getDataPath().toFile());
        OutputStream targetStream = newOutputStream(partFile.toPath())) {
      long skip = sourceStream.skip(from);
      if (skip == from) {
        IOUtils.copy(new BoundedInputStream(sourceStream, len), targetStream);
      } else {
        throw new IllegalStateException("Could not skip exact byte range");
      }
    } catch (IOException e) {
      LOG.error("Could not copy object", e);
      throw new IllegalStateException("Could not copy object", e);
    }
    return hexDigest(partFile);
  }

  private File ensurePartFile(String partNumber,
      BucketMetadata bucket,
      UUID uuid,
      String uploadId) {
    if (uuid == null) {
      return null;
    }
    File partFile = getPartPath(
        bucket,
        uuid,
        uploadId,
        partNumber).toFile();

    try {
      if (!partFile.exists() && !partFile.createNewFile()) {
        LOG.error("Could not create buffer file.");
        throw new IllegalStateException("Could not create buffer file.");
      }
    } catch (IOException e) {
      LOG.error("Could not create buffer file", e);
      throw new IllegalStateException("Could not create buffer file.", e);
    }
    return partFile;
  }

  private void verifyMultipartUploadPreparation(BucketMetadata bucket, UUID id, String uploadId) {
    Path partsFolder = null;
    MultipartUploadInfo multipartUploadInfo = uploadIdToInfo.get(uploadId);
    if (id != null) {
      partsFolder = getPartsFolderPath(bucket, id, uploadId);
    }

    if (multipartUploadInfo == null
        || partsFolder == null
        || !partsFolder.toFile().exists()
        || !partsFolder.toFile().isDirectory()) {
      LOG.error("Missed preparing Multipart Request.");
      throw new IllegalStateException("Missed preparing Multipart Request.");
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

  private boolean createPartsFolder(BucketMetadata bucket, UUID id, String uploadId) {
    File partsFolder = getPartsFolderPath(bucket, id, uploadId).toFile();
    if (!retainFilesOnExit) {
      partsFolder.deleteOnExit();
    }
    return partsFolder.mkdirs();
  }

  private Path getPartsFolderPath(BucketMetadata bucket, UUID id, String uploadId) {
    return Paths.get(bucket.getPath().toString(), id.toString(), uploadId);
  }

  private Path getPartPath(BucketMetadata bucket, UUID id, String uploadId, String partNumber) {
    return Paths.get(getPartsFolderPath(bucket, id, uploadId).toString(),
        partNumber + PART_SUFFIX);
  }

  private Path getMetaFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), META_FILE);
  }

  private Path getDataFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), DATA_FILE);
  }

  private boolean writeMetafile(BucketMetadata bucket, S3ObjectMetadata s3ObjectMetadata) {
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
