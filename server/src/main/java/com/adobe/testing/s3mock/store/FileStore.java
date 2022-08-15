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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.removeStart;

import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 Mock file store.
 */
public class FileStore {
  private static final String META_FILE = "metadata";
  private static final String DATA_FILE = "fileData";
  private static final String PART_SUFFIX = ".part";
  private static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";

  private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  private final boolean retainFilesOnExit;
  private final BucketStore bucketStore;
  private final DateTimeFormatter s3ObjectDateFormat;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Map<String, MultipartUploadInfo> uploadIdToInfo = new ConcurrentHashMap<>();

  public FileStore(boolean retainFilesOnExit, BucketStore bucketStore,
      DateTimeFormatter s3ObjectDateFormat) {
    this.retainFilesOnExit = retainFilesOnExit;
    this.bucketStore = bucketStore;
    this.s3ObjectDateFormat = s3ObjectDateFormat;
  }

  /**
   * Creates a new bucket.
   *
   * @see BucketStore#createBucket(String)
   * @deprecated use {@link BucketStore#createBucket(String)} instead.
   */
  @Deprecated //forRemoval = true
  public Bucket createBucket(final String bucketName) {
    return bucketStore.createBucket(bucketName);
  }

  /**
   * Lists all buckets managed by this FileStore.
   *
   * @see BucketStore#listBuckets()
   * @deprecated use {@link BucketStore#listBuckets()} instead.
   */
  @Deprecated //forRemoval = true
  public List<Bucket> listBuckets() {
    return bucketStore.listBuckets();
  }

  /**
   * Retrieves a bucket identified by its name.
   *
   * @see BucketStore#getBucket(String)
   * @deprecated use {@link BucketStore#getBucket(String)} instead.
   */
  @Deprecated //forRemoval = true
  public Bucket getBucket(final String bucketName) {
    return bucketStore.getBucket(bucketName);
  }

  /**
   * Stores a File inside a Bucket.
   *
   * @see FileStore#putS3Object(String, String, String, String, InputStream, boolean,
   *      Map, String, String)
   * @deprecated This method is only used in S3Mock tests.
   */
  @Deprecated //forRemoval = true
  public S3Object putS3Object(final String bucketName,
      final String fileName,
      final String contentType,
      final String contentEncoding,
      final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat) throws IOException {
    return putS3Object(bucketName, fileName, contentType, contentEncoding,
        dataStream, useV4ChunkedWithSigningFormat, Collections.emptyMap(), null, null);
  }

  /**
   * Stores a File inside a Bucket.
   *
   * @see FileStore#putS3Object(String, String, String, String, InputStream, boolean,
   *      Map, String, String)
   * @deprecated This method is not used in S3Mock.
   */
  @Deprecated //forRemoval = true
  public S3Object putS3Object(final String bucketName,
      final String fileName,
      final String contentType,
      final String contentEncoding,
      final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat,
      final Map<String, String> userMetadata) throws IOException {
    return putS3Object(bucketName, fileName, contentType, contentEncoding,
        dataStream, useV4ChunkedWithSigningFormat, userMetadata, null, null);
  }

  /**
   * Generically stores a File inside a Bucket.
   *
   * @param bucketName Bucket to store the File in.
   * @param fileName name of the File to be stored.
   * @param contentType The files Content Type.
   * @param contentEncoding The files Content Encoding.
   * @param dataStream The File as InputStream.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return {@link S3Object}.
   *
   * @throws IOException if an I/O error occurs.
   */
  public S3Object putS3Object(final String bucketName,
      final String fileName,
      final String contentType,
      final String contentEncoding,
      final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat,
      final Map<String, String> userMetadata,
      final String encryption, final String kmsKeyId) throws IOException {
    boolean encrypted = isNotBlank(encryption) && isNotBlank(kmsKeyId);
    final S3Object s3Object = new S3Object();
    s3Object.setName(fileName);
    s3Object.setContentType(contentType != null ? contentType : DEFAULT_CONTENT_TYPE);
    s3Object.setContentEncoding(contentEncoding);
    s3Object.setUserMetadata(userMetadata);
    s3Object.setEncrypted(encrypted);
    s3Object.setKmsEncryption(encryption);
    s3Object.setKmsEncryptionKeyId(kmsKeyId);

    createObjectRootFolder(bucketName, s3Object.getName());
    final File dataFile =
        inputStreamToFile(wrapStream(dataStream, useV4ChunkedWithSigningFormat),
            getDataFilePath(bucketName, fileName));
    s3Object.setDataFile(dataFile);

    s3Object.setSize(Long.toString(dataFile.length()));

    final BasicFileAttributes attributes =
        Files.readAttributes(dataFile.toPath(), BasicFileAttributes.class);
    s3Object.setCreationDate(
        s3ObjectDateFormat.format(attributes.creationTime().toInstant()));
    s3Object.setModificationDate(
        s3ObjectDateFormat.format(attributes.lastModifiedTime().toInstant()));
    s3Object.setLastModified(attributes.lastModifiedTime().toMillis());

    s3Object.setEtag(digest(kmsKeyId, dataFile));

    File metaFile = getMetaFilePath(bucketName, fileName).toFile();
    if (!retainFilesOnExit) {
      metaFile.deleteOnExit();
    }
    objectMapper.writeValue(metaFile, s3Object);

    return s3Object;
  }

  /**
   * Stores an encrypted File inside a Bucket.
   *
   * @see FileStore#putS3Object(String, String, String, String, InputStream, boolean,
   *      Map, String, String)
   * @deprecated This method is not used in S3Mock.
   */
  @Deprecated //forRemoval = true
  public S3Object putS3ObjectWithKMSEncryption(final String bucketName,
      final String fileName,
      final String contentType,
      final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat,
      final String encryption, final String kmsKeyId) throws IOException {
    return putS3Object(bucketName, fileName, contentType, null, dataStream,
        useV4ChunkedWithSigningFormat, Collections.emptyMap(), encryption, kmsKeyId);
  }

  /**
   * Stores an encrypted File inside a Bucket.
   *
   * @see FileStore#putS3Object(String, String, String, String, InputStream, boolean,
   *      Map, String, String)
   * @deprecated This method is not used in S3Mock.
   */
  @Deprecated //forRemoval = true
  public S3Object putS3ObjectWithKMSEncryption(final String bucketName,
      final String fileName,
      final String contentType,
      final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat,
      final Map<String, String> userMetadata,
      final String encryption, final String kmsKeyId) throws IOException {
    return putS3Object(bucketName, fileName, contentType, null, dataStream,
        useV4ChunkedWithSigningFormat, userMetadata, encryption, kmsKeyId);
  }

  private InputStream wrapStream(final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat) {
    final InputStream inStream;
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
   * @param bucketName Bucket where the file is stored in.
   * @param fileName name of the file to which tags have to be attached.
   * @param tags List of tag objects.
   *
   * @throws IOException if an I/O error occurs.
   */
  public void setObjectTags(final String bucketName,
      final String fileName,
      final List<Tag> tags) throws IOException {
    final S3Object s3Object = getS3Object(bucketName, fileName);
    s3Object.setTags(tags);
    objectMapper.writeValue(getMetaFilePath(bucketName, fileName).toFile(), s3Object);
  }

  /**
   * Sets user metadata for a given object.
   *
   * @param bucketName Bucket where the file is stored in.
   * @param fileName name of the file to which tags have to be attached.
   * @param metadata Map of metadata.
   *
   * @throws IOException if an I/O error occurs.
   */
  public void setUserMetadata(final String bucketName,
      final String fileName,
      final Map<String, String> metadata) throws IOException {
    final S3Object s3Object = getS3Object(bucketName, fileName);
    s3Object.setUserMetadata(metadata);
    objectMapper.writeValue(getMetaFilePath(bucketName, fileName).toFile(), s3Object);
  }

  /**
   * Stores the Content of an InputStream in a File Creates File if it not exists.
   *
   * @param inputStream the Stream to be saved.
   * @param filePath Path where the stream should be saved.
   *
   * @return the newly created File.
   */
  private File inputStreamToFile(final InputStream inputStream, final Path filePath) {
    OutputStream outputStream = null;
    final File targetFile = filePath.toFile();
    try {
      if (!targetFile.exists()) {
        if (targetFile.createNewFile()) {
          if (!retainFilesOnExit) {
            targetFile.deleteOnExit();
          }
        }
      }

      outputStream = Files.newOutputStream(targetFile.toPath());
      int read;
      final byte[] bytes = new byte[1024];

      while ((read = inputStream.read(bytes)) != -1) {
        outputStream.write(bytes, 0, read);
      }

    } catch (final IOException e) {
      LOG.error("Wasn't able to store file on disk!", e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (final IOException e) {
          LOG.error("InputStream can not be closed!", e);
        }
      }
      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (final IOException e) {
          LOG.error("OutputStream can not be closed!", e);
        }
      }
    }
    return targetFile;
  }

  /**
   * Normalizes provided prefix in context of the bucket's underlying file system.
   *
   * @param bucket the Bucket in which normalization of the path should happen.
   * @param prefix prefix to be normalized
   *
   * @return normalized prefix containing slashes flipped the right way
   */
  private String normalizePrefix(Bucket bucket, String prefix) {
    if (prefix == null) {
      return null;
    }
    FileSystem fileSystem = bucket.getPath().getFileSystem();
    String normalized = fileSystem.getPath(prefix).toString();
    //check if there was a trailing slash removed
    return (normalized.length() != prefix.length()
        ? normalized + fileSystem.getSeparator() : normalized);
  }

  /**
   * Retrieves an Object from a bucket.
   *
   * @param bucketName the Bucket in which to look the file in.
   * @param objectName name of the object.
   *
   * @return the retrieved S3Object or null if not found
   */
  public S3Object getS3Object(final String bucketName, final String objectName) {
    S3Object theObject = null;

    final Path metaPath = getMetaFilePath(bucketName, objectName);

    if (Files.exists(metaPath)) {
      try {
        theObject = objectMapper.readValue(metaPath.toFile(), S3Object.class);
        theObject.setDataFile(getDataFilePath(bucketName, objectName).toFile());
      } catch (final IOException e) {
        LOG.error("File can not be read", e);
        e.printStackTrace();
      }
    }
    return theObject;
  }

  /**
   * Retrieves list of Objects from a bucket.
   *
   * @param bucketName the Bucket in which to list the file(s) in.
   * @param prefix {@link String} object file name starts with
   *
   * @return the retrieved {@code List<S3Object>} or null if not found
   *
   * @throws IOException if directory stream fails
   */
  public List<S3Object> getS3Objects(final String bucketName, final String prefix)
      throws IOException {

    final Bucket theBucket = getBucket(requireNonNull(bucketName, "bucketName == null"));

    final List<S3Object> resultObjects = new ArrayList<>();

    final String normalizedPrefix = normalizePrefix(theBucket, prefix);

    final Set<Path> collect;
    try (Stream<Path> directoryHierarchy = Files.walk(theBucket.getPath())) {

      collect = directoryHierarchy
          .filter(path -> path.toFile().isDirectory())
          .map(path -> theBucket.getPath().relativize(path))
          .filter(path -> isBlank(prefix)
              || (null != normalizedPrefix
              // match by prefix...
              && path.toString().startsWith(normalizedPrefix)))
          .collect(toSet());
    }

    for (final Path path : collect) {
      final S3Object s3Object = getS3Object(bucketName, path.toString());
      if (s3Object != null) {
        resultObjects.add(s3Object);
      }
    }

    return resultObjects;
  }

  /**
   * Copies an object, identified by bucket and name, to another bucket and objectName.
   *
   * @param sourceBucketName name of the bucket to copy from.
   * @param sourceObjectName name of the object to copy.
   * @param destinationBucketName name of the destination bucket.
   * @param destinationObjectName name of the destination object.
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   *
   * @throws FileNotFoundException no FileInputStream of the sourceFile can be created.
   * @throws IOException If File can't be read.
   */
  public CopyObjectResult copyS3Object(final String sourceBucketName,
      final String sourceObjectName,
      final String destinationBucketName,
      final String destinationObjectName) throws IOException {
    return copyS3ObjectEncrypted(sourceBucketName, sourceObjectName, destinationBucketName,
        destinationObjectName, null, null, Collections.emptyMap());
  }

  /**
   * Copies an object, identified by bucket and name, to another bucket and objectName.
   *
   * @see FileStore#copyS3Object(String, String, String, String)
   * @deprecated This method is not used in S3Mock.
   */
  @Deprecated //forRemoval = true
  public CopyObjectResult copyS3Object(final String sourceBucketName,
      final String sourceObjectName,
      final String destinationBucketName,
      final String destinationObjectName,
      final Map<String, String> userMetadata) throws IOException {
    return copyS3ObjectEncrypted(sourceBucketName, sourceObjectName, destinationBucketName,
        destinationObjectName, null, null, userMetadata);
  }

  /**
   * Copies an object to another bucket and encrypted object.
   *
   * @param sourceBucketName name of the bucket to copy from.
   * @param sourceObjectName name of the object to copy.
   * @param destinationBucketName name of the destination bucket.
   * @param destinationObjectName name of the destination object.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   *
   * @throws FileNotFoundException no FileInputStream of the sourceFile can be created.
   * @throws IOException If File can't be read.
   */
  public CopyObjectResult copyS3ObjectEncrypted(final String sourceBucketName,
      final String sourceObjectName,
      final String destinationBucketName,
      final String destinationObjectName,
      final String encryption, final String kmsKeyId) throws IOException {
    return copyS3ObjectEncrypted(sourceBucketName, sourceObjectName, destinationBucketName,
        destinationObjectName, encryption, kmsKeyId, Collections.emptyMap());
  }

  /**
   * Copies an object to another bucket and encrypted object.
   *
   * @param sourceBucketName name of the bucket to copy from.
   * @param sourceObjectName name of the object to copy.
   * @param destinationBucketName name of the destination bucket.
   * @param destinationObjectName name of the destination object.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   * @param userMetadata User metadata to store for destination object
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   *
   * @throws FileNotFoundException no FileInputStream of the sourceFile can be created.
   * @throws IOException If File can't be read.
   */
  public CopyObjectResult copyS3ObjectEncrypted(final String sourceBucketName,
      final String sourceObjectName,
      final String destinationBucketName,
      final String destinationObjectName,
      final String encryption,
      final String kmsKeyId,
      final Map<String, String> userMetadata) throws IOException {
    final S3Object sourceObject = getS3Object(sourceBucketName, sourceObjectName);
    if (sourceObject == null) {
      return null;
    }
    Map<String, String> copyUserMetadata = sourceObject.getUserMetadata();
    if (userMetadata != null && !userMetadata.isEmpty()) {
      //if userMetadata is passed in, it's used to REPLACE existing userMetadata
      copyUserMetadata = userMetadata;
    }
    if (sourceObjectName.equals(destinationObjectName)
        && sourceBucketName.equals(destinationBucketName)) {
      // source and destination is the same, pretend we copied - S3 does the same.
      // this does not change the modificationDate. Also, this would need to increment the
      // version if/when we support versioning.

      // overwrite metadata if necessary.
      setUserMetadata(sourceBucketName, sourceObjectName, copyUserMetadata);

      return new CopyObjectResult(sourceObject.getModificationDate(), sourceObject.getEtag());
    }

    final S3Object copiedObject =
        putS3Object(destinationBucketName,
            destinationObjectName,
            sourceObject.getContentType(),
            sourceObject.getContentEncoding(),
            Files.newInputStream(sourceObject.getDataFile().toPath()),
            false,
            copyUserMetadata,
            encryption,
            kmsKeyId);

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getEtag());
  }

  /**
   * Checks if the specified bucket exists.
   *
   * @see BucketStore#doesBucketExist(String)
   * @deprecated use {@link  BucketStore#doesBucketExist(String)} instead.
   */
  @Deprecated //forRemoval = true
  public Boolean doesBucketExist(final String bucketName) {
    return bucketStore.doesBucketExist(bucketName);
  }

  /**
   * Removes an object from a bucket.
   *
   * @param bucketName name of the bucket containing the object.
   * @param objectName name of the object to be deleted.
   *
   * @return true if deletion succeeded.
   *
   * @throws IOException if File could not be accessed.
   */
  public boolean deleteObject(final String bucketName, final String objectName) throws IOException {
    final S3Object s3Object = getS3Object(bucketName, objectName);
    if (s3Object != null) {
      FileUtils.deleteDirectory(s3Object.getDataFile().getParentFile());
      return true;
    } else {
      return false;
    }
  }

  /**
   * Deletes a Bucket and all of its contents.
   *
   * @see BucketStore#deleteBucket(String)
   * @deprecated use {@link BucketStore#deleteBucket(String)} instead.
   */
  @Deprecated //forRemoval = true
  public boolean deleteBucket(final String bucketName) throws IOException {
    return bucketStore.deleteBucket(bucketName);
  }

  /**
   * Prepares everything to store files uploaded as multipart upload.
   *
   * @param bucketName in which to upload
   * @param fileName of the file to upload
   * @param contentType the content type
   * @param contentEncoding the content encoding
   * @param uploadId id of the upload
   * @param owner owner of the upload
   * @param initiator initiator of the upload
   * @param userMetadata custom metadata
   *
   * @return upload result
   */
  public MultipartUpload prepareMultipartUpload(final String bucketName, final String fileName,
      final String contentType, final String contentEncoding, final String uploadId,
      final Owner owner, final Owner initiator, final Map<String, String> userMetadata) {

    if (!createPartsFolder(bucketName, fileName, uploadId)) {
      throw new IllegalStateException(
          "Directories for storing multipart uploads couldn't be created.");
    }
    final MultipartUpload upload =
        new MultipartUpload(fileName, uploadId, owner, initiator, new Date());
    uploadIdToInfo.put(uploadId, new MultipartUploadInfo(upload,
        contentType, contentEncoding, userMetadata, bucketName));

    return upload;
  }

  /**
   * Prepares everything to store files uploaded as multipart upload.
   *
   * @param bucketName in which to upload
   * @param fileName of the file to upload
   * @param contentType the content type
   * @param contentEncoding the content encoding
   * @param uploadId id of the upload
   * @param owner owner of the upload
   * @param initiator initiator of the upload
   *
   * @return upload result
   */
  public MultipartUpload prepareMultipartUpload(final String bucketName, final String fileName,
      final String contentType, final String contentEncoding, final String uploadId,
      final Owner owner, final Owner initiator) {

    return prepareMultipartUpload(bucketName, fileName, contentType, contentEncoding, uploadId,
        owner, initiator, Collections.emptyMap());
  }

  /**
   * Lists the not-yet completed parts of a multipart upload across all buckets.
   *
   * @see #listMultipartUploads(String)
   * @deprecated use {@link #listMultipartUploads(String)} with null as parameter instead.
   */
  @Deprecated //forRemoval = true
  public Collection<MultipartUpload> listMultipartUploads() {
    return listMultipartUploads(null);
  }

  /**
   * Lists the not-yet completed parts of a multipart upload.
   *
   * @return the list of not-yet completed multipart uploads.
   */
  public Collection<MultipartUpload> listMultipartUploads(String bucketName) {
    return uploadIdToInfo.values()
        .stream()
        .filter(info -> bucketName == null || bucketName.equals(info.bucket))
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
   * @param bucketName to which was uploaded
   * @param fileName which was uploaded
   * @param uploadId of the upload
   */
  public void abortMultipartUpload(final String bucketName, final String fileName,
      final String uploadId) {

    synchronizedUpload(uploadId, uploadInfo -> {

      try {
        final File partFolder = retrieveFile(bucketName, fileName, uploadId);
        FileUtils.deleteDirectory(partFolder);

        final File entireFile = retrieveFile(bucketName, fileName, DATA_FILE);
        FileUtils.deleteQuietly(entireFile);

        uploadIdToInfo.remove(uploadId);

        return null;
      } catch (final IOException e) {
        throw new IllegalStateException("Could not delete multipart upload tmp data.", e);
      }
    });
  }

  /**
   * Uploads a part of a multipart upload.
   *
   * @param bucketName in which to upload
   * @param fileName of the file to upload
   * @param uploadId id of the upload
   * @param partNumber number of the part to store
   * @param inputStream file data to be stored
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   *
   * @return the md5 hash of this part
   *
   * @throws IOException if file could not be read to calculate digest
   */
  public String putPart(final String bucketName,
      final String fileName,
      final String uploadId,
      final String partNumber,
      final InputStream inputStream,
      final boolean useV4ChunkedWithSigningFormat) throws IOException {
    try (final DigestInputStream digestingInputStream =
        new DigestInputStream(wrapStream(inputStream, useV4ChunkedWithSigningFormat),
            MessageDigest.getInstance("MD5"))) {
      inputStreamToFile(digestingInputStream,
          getPartPath(bucketName, fileName, uploadId, partNumber));

      return new String(Hex.encodeHex(digestingInputStream.getMessageDigest().digest()));
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Finishes the upload.
   *
   * @param bucketName to which was uploaded.
   * @param fileName which was uploaded.
   * @param uploadId of the upload.
   * @param parts to concatenate.
   *
   * @return the etag of the complete file.
   */
  public String completeMultipartUpload(final String bucketName, final String fileName,
      final String uploadId, final List<CompletedPart> parts) {

    return completeMultipartUpload(bucketName, fileName, uploadId, parts, null, null);
  }

  /**
   * Completes a Multipart Upload for the given ID.
   *
   * @param bucketName in which to upload.
   * @param fileName of the file to upload.
   * @param uploadId id of the upload.
   * @param parts to concatenate.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return etag of the uploaded file.
   */
  public String completeMultipartUpload(final String bucketName, final String fileName,
      final String uploadId, final List<CompletedPart> parts, final String encryption,
      final String kmsKeyId) {

    return synchronizedUpload(uploadId, uploadInfo -> {

      final S3Object s3Object = new S3Object();
      s3Object.setName(fileName);

      s3Object.setEncrypted(encryption != null || kmsKeyId != null);
      if (encryption != null) {
        s3Object.setKmsEncryption(encryption);
      }

      if (kmsKeyId != null) {
        s3Object.setKmsEncryptionKeyId(kmsKeyId);
      }

      final File partFolder = retrieveFile(bucketName, fileName, uploadId);
      final File entireFile = retrieveFile(bucketName, fileName, DATA_FILE);

      final String[] partNames =
          parts.stream().map(part -> part.getPartNumber() + PART_SUFFIX).toArray(String[]::new);

      final long size = writeEntireFile(entireFile, partFolder, partNames);

      try {
        final byte[] allMd5s = concatenateMd5sForAllParts(partFolder, partNames);
        FileUtils.deleteDirectory(partFolder);

        final BasicFileAttributes attributes =
            Files.readAttributes(entireFile.toPath(), BasicFileAttributes.class);
        s3Object.setCreationDate(s3ObjectDateFormat.format(
            attributes.creationTime().toInstant()));
        s3Object.setModificationDate(s3ObjectDateFormat.format(
            attributes.lastModifiedTime().toInstant()));
        s3Object.setLastModified(attributes.lastModifiedTime().toMillis());
        s3Object.setEtag(DigestUtils.md5Hex(allMd5s) + "-" + partNames.length);
        s3Object.setSize(Long.toString(size));
        s3Object.setContentType(
            uploadInfo.contentType != null ? uploadInfo.contentType : DEFAULT_CONTENT_TYPE);
        s3Object.setContentEncoding(uploadInfo.contentEncoding);
        s3Object.setUserMetadata(uploadInfo.userMetadata);

        uploadIdToInfo.remove(uploadId);

      } catch (final IOException e) {
        throw new IllegalStateException("Error finishing multipart upload", e);
      }

      try {
        objectMapper.writeValue(
            getMetaFilePath(bucketName, fileName).toFile(),
            s3Object);
      } catch (final IOException e) {
        throw new IllegalStateException("Could not write metadata-file", e);
      }

      return s3Object.getEtag();
    });
  }

  private String[] listAndSortPartsInFromDirectory(final File partFolder) {
    final String[] partNames = partFolder.list((dir, name) -> name.endsWith(PART_SUFFIX));

    Arrays.sort(partNames != null ? partNames : new String[0],
        Comparator.comparingInt(s -> Integer.parseInt(s.substring(0, s.indexOf(PART_SUFFIX)))));
    return partNames;
  }

  /**
   * Calculates the MD5 for each part and concatenates the result to a large array.
   *
   * @param partFolder the folder where all parts are located.
   * @param partNames the name of each part file
   *
   * @return a byte array containing all md5 bytes for each part concatenated.
   *
   * @throws IOException if a part file could not be read.
   */
  private byte[] concatenateMd5sForAllParts(final File partFolder, final String[] partNames)
      throws IOException {
    byte[] allMd5s = new byte[0];
    for (final String partName : partNames) {
      try (final InputStream inputStream =
          Files.newInputStream(Paths.get(partFolder.getAbsolutePath(), partName))) {
        allMd5s = ArrayUtils.addAll(allMd5s, DigestUtils.md5(inputStream));
      }
    }
    return allMd5s;
  }

  /**
   * Get all multipart upload parts.
   * @param bucketName name of the bucket
   * @param fileName name of the file (object key)
   * @param uploadId upload identifier
   * @return Array of Files
   */
  public List<Part> getMultipartUploadParts(final String bucketName,
      final String fileName,
      final String uploadId) {
    final File partsDirectory = retrieveFile(bucketName, fileName, uploadId);
    final String[] partNames = listAndSortPartsInFromDirectory(partsDirectory);

    if (partNames != null) {
      final File[] files = Arrays.stream(partNames).map(File::new).toArray(File[]::new);
      return arrangeSeparateParts(files, bucketName, fileName, uploadId);
    } else {
      return Collections.emptyList();
    }
  }

  private File retrieveFile(final String bucketName, final String fileName, final String uploadId) {
    return getPartsFolderPath(bucketName, fileName, uploadId).toFile();
  }

  private List<Part> arrangeSeparateParts(final File[] files, final String bucketName,
      final String fileName, final String uploadId) {

    final List<Part> parts = new ArrayList<>();

    for (int i = 0; i < files.length; i++) {
      final String filePartPath = concatUploadIdAndPartFileName(files[i], uploadId);

      final File currentFilePart = retrieveFile(bucketName, fileName, filePartPath);

      final int partNumber = i + 1;
      final String partMd5 = calculateDigestOfFilePart(currentFilePart);
      final Date lastModified = new Date(currentFilePart.lastModified());

      final Part part = new Part();

      part.setLastModified(lastModified);
      part.setETag(partMd5);
      part.setPartNumber((partNumber));
      part.setSize(currentFilePart.length());

      parts.add(part);
    }

    return parts;
  }

  private String calculateDigestOfFilePart(final File currentFilePart) {
    try (final InputStream is = FileUtils.openInputStream(currentFilePart)) {
      final String partMd5 = DigestUtils.md5Hex(is);
      return String.format("%s", partMd5);
    } catch (final IOException e) {
      LOG.error("Digest could not be calculated. File access did not succeed", e);
      return "";
    }
  }

  private String concatUploadIdAndPartFileName(final File file, final String uploadId) {
    return String.format("%s/%s", uploadId, file.getName());
  }

  private long writeEntireFile(final File entireFile, final File partFolder,
      final String... partNames) {
    try (final OutputStream targetStream = Files.newOutputStream(entireFile.toPath())) {
      long size = 0;
      for (final String partName : partNames) {
        size += Files.copy(Paths.get(partFolder.getAbsolutePath(), partName), targetStream);
      }
      return size;
    } catch (final IOException e) {
      throw new IllegalStateException("Error writing entire file "
          + entireFile.getAbsolutePath(), e);
    }
  }

  /**
   * Synchronize access on the upload, to handle concurrent abortion/completion.
   */
  private <T> T synchronizedUpload(final String uploadId,
      final Function<MultipartUploadInfo, T> callback) {

    final MultipartUploadInfo uploadInfo = uploadIdToInfo.get(uploadId);
    if (uploadInfo == null) {
      throw new IllegalArgumentException("Unknown upload " + uploadId);
    }

    // we assume that an uploadId -> uploadInfo is only registered once and not modified in between,
    // therefore we can synchronize on the uploadInfo instance
    synchronized (uploadInfo) {

      // check if the upload was aborted or completed in the meantime
      if (!uploadIdToInfo.containsKey(uploadId)) {
        throw new IllegalStateException(
            "Upload " + uploadId + " was aborted or completed concurrently");
      }

      return callback.apply(uploadInfo);

    }
  }

  private String digest(final String salt, final File dataFile) throws IOException {
    try (final FileInputStream inputStream = new FileInputStream(dataFile)) {
      return DigestUtil.getHexDigest(salt, inputStream);
    } catch (final NoSuchAlgorithmException e) {
      LOG.error("Digest can not be calculated!", e);
      return null;
    }
  }

  /**
   * Copies the range, define by from/to, from the S3 Object, identified by the given key to given
   * destination into the given bucket.
   *
   * @param bucket The source Bucket.
   * @param key Identifies the S3 Object.
   * @param copyRange Byte range to copy. Optional.
   * @param partNumber The part to copy.
   * @param destinationBucket The Bucket the target file (will) reside in.
   * @param destinationFilename The target file.
   * @param uploadId id of the upload.
   *
   * @return etag of the uploaded file.
   *
   * @throws IOException When writing the file fails.
   */
  public String copyPart(final String bucket,
      final String key,
      final Range copyRange,
      final String partNumber,
      final String destinationBucket,
      final String destinationFilename,
      final String uploadId) throws IOException {

    verifyMultipartUploadPreparation(destinationBucket, destinationFilename, uploadId);

    final File targetPartFile =
        ensurePartFile(partNumber, destinationBucket, destinationFilename, uploadId);

    return copyPart(bucket, key, copyRange, targetPartFile);
  }

  private String copyPart(final String bucket,
      final String key,
      final Range copyRange,
      final File partFile) throws IOException {
    long from = 0;
    final S3Object s3Object = resolveS3Object(bucket, key);
    long len = s3Object.getDataFile().length();
    if (copyRange != null) {
      from = copyRange.getStart();
      len = copyRange.getEnd() - copyRange.getStart() + 1;
    }

    try (final InputStream sourceStream = FileUtils.openInputStream(s3Object.getDataFile());
        final OutputStream targetStream = Files.newOutputStream(partFile.toPath())) {
      sourceStream.skip(from);
      IOUtils.copy(new BoundedInputStream(sourceStream, len), targetStream);
    }
    try (final InputStream is = FileUtils.openInputStream(partFile)) {
      return DigestUtils.md5Hex(is);
    }
  }

  private File ensurePartFile(final String partNumber,
      final String destinationBucket,
      final String destinationFilename,
      final String uploadId) throws IOException {
    final File partFile = getPartPath(
        destinationBucket,
        destinationFilename,
        uploadId,
        partNumber).toFile();

    if (!partFile.exists() && !partFile.createNewFile()) {
      throw new IllegalStateException("Could not create buffer file");
    }
    return partFile;
  }

  private void verifyMultipartUploadPreparation(final String destinationBucket,
      final String destinationFilename, final String uploadId) {
    MultipartUploadInfo multipartUploadInfo = uploadIdToInfo.get(uploadId);
    final Path partsFolder = getPartsFolderPath(destinationBucket, destinationFilename, uploadId);

    if (multipartUploadInfo == null
        || !partsFolder.toFile().exists()
        || !partsFolder.toFile().isDirectory()) {
      throw new IllegalStateException("Missed preparing Multipart Request");
    }
  }

  /**
   * Creates the root folder in which to store data and meta file.
   *
   * @param bucketName the Bucket containing the Object.
   * @param objectName name of the object to be stored.
   *
   * @return The Folder to store the Object in.
   */
  private boolean createObjectRootFolder(final String bucketName, final String objectName) {
    final File objectRootFolder = getObjectFolderPath(bucketName, objectName).toFile();
    return objectRootFolder.mkdirs();
  }

  private Path getObjectFolderPath(final String bucketName, final String fileName) {
    final Bucket bucket = getBucket(bucketName);
    return Paths.get(bucket.getPath().toString(), fileName);
  }

  private boolean createPartsFolder(final String bucketName, final String fileName,
      final String uploadId) {
    File partsFolder = getPartsFolderPath(bucketName, fileName, uploadId).toFile();
    if (!retainFilesOnExit) {
      partsFolder.deleteOnExit();
    }
    return partsFolder.mkdirs();
  }

  private Path getPartsFolderPath(final String bucketName, final String fileName,
      final String uploadId) {
    final Bucket bucket = getBucket(bucketName);
    return Paths.get(bucket.getPath().toString(), fileName, uploadId);
  }

  private Path getPartPath(final String bucketName, final String fileName,
      final String uploadId, final String partNumber) {
    return Paths.get(getPartsFolderPath(bucketName, fileName, uploadId).toString(),
        partNumber + PART_SUFFIX);
  }

  private Path getMetaFilePath(final String bucketName, final String fileName) {
    // Path can't be resolved in the local bucket root if it's absolute.
    // TODO: do we still need this?
    final String relativeName = removeStart(fileName, "/");
    return Paths.get(getObjectFolderPath(bucketName, relativeName).toString(), META_FILE);
  }

  private Path getDataFilePath(final String bucketName, final String fileName) {
    // Path can't be resolved in the local bucket root if it's absolute.
    // TODO: do we still need this?
    final String relativeName = removeStart(fileName, "/");
    return Paths.get(getObjectFolderPath(bucketName, relativeName).toString(), DATA_FILE);
  }

  private S3Object resolveS3Object(final String bucket, final String key) {
    final S3Object s3Object = getS3Object(bucket, key);

    if (s3Object == null) {
      throw new IllegalStateException("Source Object not found");
    }
    return s3Object;
  }
}
