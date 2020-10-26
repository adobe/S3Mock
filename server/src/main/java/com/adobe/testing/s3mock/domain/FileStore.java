/*
 *  Copyright 2017-2020 Adobe.
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

package com.adobe.testing.s3mock.domain;

import static com.adobe.testing.s3mock.S3MockApplication.PROP_ROOT_DIRECTORY;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.springframework.util.StringUtils.isEmpty;

import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.util.AwsChunkDecodingInputStream;
import com.adobe.testing.s3mock.util.HashUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * S3 Mock file store.
 */
@Component
public class FileStore {

  private static final SimpleDateFormat S3_OBJECT_DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.000Z'");

  private static final String META_FILE = "metadata";
  private static final String DATA_FILE = "fileData";
  private static final String PART_SUFFIX = ".part";
  private static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";

  private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  private final File rootFolder;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Map<String, MultipartUploadInfo> uploadIdToInfo = new ConcurrentHashMap<>();

  /**
   * Constructs a new {@link FileStore}.
   *
   * @param rootDirectory The directory to use. If omitted, a temp directory will be used.
   */
  public FileStore(@Value("${" + PROP_ROOT_DIRECTORY + ":}") final String rootDirectory) {
    rootFolder = createRootFolder(rootDirectory);
  }

  private File createRootFolder(final String rootDirectory) {
    final File root;
    if (rootDirectory == null || rootDirectory.isEmpty()) {
      root = new File(FileUtils.getTempDirectory(), "s3mockFileStore" + new Date().getTime());
    } else {
      root = new File(rootDirectory);
    }

    root.deleteOnExit();
    root.mkdir();

    return root;
  }

  /**
   * Creates a new bucket.
   *
   * @param bucketName name of the Bucket to be created.
   *
   * @return the newly created Bucket.
   *
   * @throws IOException if the bucket cannot be created or the bucket already exists but is not
   *     a directory.
   */
  public Bucket createBucket(final String bucketName) throws IOException {
    final File newBucket = new File(rootFolder, bucketName);
    FileUtils.forceMkdir(newBucket);

    return bucketFromPath(newBucket.toPath());
  }

  /**
   * Lists all buckets managed by this FileStore.
   *
   * @return List of all Buckets.
   */
  public List<Bucket> listBuckets() {
    final DirectoryStream.Filter<Path> filter = file -> (Files.isDirectory(file));

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

  private Bucket bucketFromPath(final Path path) {
    Bucket result = null;
    final BasicFileAttributes attributes;
    try {
      attributes = Files.readAttributes(path, BasicFileAttributes.class);
      result =
          new Bucket(path,
              path.getFileName().toString(),
              S3_OBJECT_DATE_FORMAT.format(new Date(attributes.creationTime().toMillis())));
    } catch (final IOException e) {
      LOG.error("File can not be read!", e);
    }

    return result;
  }

  /**
   * Stores a File inside a Bucket.
   *
   * @param bucketName Bucket to store the File in.
   * @param fileName name of the File to be stored.
   * @param contentType The files Content Type.
   * @param contentEncoding The files Content Encoding.
   * @param dataStream The File as InputStream.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
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
      final boolean useV4ChunkedWithSigningFormat) throws IOException {
    return putS3Object(bucketName, fileName, contentType, contentEncoding, dataStream,
        useV4ChunkedWithSigningFormat, Collections.emptyMap());
  }

  /**
   * Stores a File inside a Bucket.
   *
   * @param bucketName Bucket to store the File in.
   * @param fileName name of the File to be stored.
   * @param contentType The files Content Type.
   * @param contentEncoding The files Content Encoding.
   * @param dataStream The File as InputStream.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
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
      final Map<String, String> userMetadata) throws IOException {
    final S3Object s3Object = new S3Object();
    s3Object.setName(fileName);
    s3Object.setContentType(contentType != null ? contentType : DEFAULT_CONTENT_TYPE);
    s3Object.setContentEncoding(contentEncoding);
    s3Object.setUserMetadata(userMetadata);

    final Bucket theBucket = getBucketOrCreateNewOne(bucketName);

    final File objectRootFolder = createObjectRootFolder(theBucket, s3Object.getName());

    final File dataFile =
        inputStreamToFile(wrapStream(dataStream, useV4ChunkedWithSigningFormat),
            objectRootFolder.toPath().resolve(DATA_FILE));
    s3Object.setDataFile(dataFile);
    s3Object.setSize(Long.toString(dataFile.length()));

    final BasicFileAttributes attributes =
        Files.readAttributes(dataFile.toPath(), BasicFileAttributes.class);
    s3Object.setCreationDate(
        S3_OBJECT_DATE_FORMAT.format(new Date(attributes.creationTime().toMillis())));
    s3Object.setModificationDate(
        S3_OBJECT_DATE_FORMAT.format(new Date(attributes.lastModifiedTime().toMillis())));
    s3Object.setLastModified(attributes.lastModifiedTime().toMillis());

    s3Object.setMd5(digest(null, dataFile));

    objectMapper.writeValue(new File(objectRootFolder, META_FILE), s3Object);

    return s3Object;
  }

  private InputStream wrapStream(final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat) {
    final InputStream inStream;
    if (useV4ChunkedWithSigningFormat) {
      inStream = new AwsChunkDecodingInputStream(dataStream);
    } else {
      inStream = dataStream;
    }

    return inStream;
  }

  /**
   * Stores an encrypted File inside a Bucket.
   *
   * @param bucketName Bucket to store the File in.
   * @param fileName name of the File to be stored.
   * @param contentType The files Content Type.
   * @param dataStream The File as InputStream.
   * @param useV4Signing If {@code true}, V4-style signing is enabled.
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return {@link S3Object}.
   *
   * @throws IOException if an I/O error occurs.
   */
  public S3Object putS3ObjectWithKMSEncryption(final String bucketName,
      final String fileName,
      final String contentType,
      final InputStream dataStream,
      final boolean useV4Signing,
      final String encryption, final String kmsKeyId) throws IOException {
    return putS3ObjectWithKMSEncryption(bucketName, fileName, contentType, dataStream, useV4Signing,
        Collections.emptyMap(), encryption, kmsKeyId);
  }

  /**
   * Stores an encrypted File inside a Bucket.
   *
   * @param bucketName Bucket to store the File in.
   * @param fileName name of the File to be stored.
   * @param contentType The files Content Type.
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
  public S3Object putS3ObjectWithKMSEncryption(final String bucketName,
      final String fileName,
      final String contentType,
      final InputStream dataStream,
      final boolean useV4ChunkedWithSigningFormat,
      final Map<String, String> userMetadata,
      final String encryption, final String kmsKeyId) throws IOException {
    final S3Object s3Object = new S3Object();
    s3Object.setName(fileName);
    s3Object.setContentType(contentType);
    s3Object.setUserMetadata(userMetadata);
    s3Object.setEncrypted(true);
    s3Object.setKmsEncryption(encryption);
    s3Object.setKmsEncryptionKeyId(kmsKeyId);

    final Bucket theBucket = getBucketOrCreateNewOne(bucketName);

    final File objectRootFolder = createObjectRootFolder(theBucket, s3Object.getName());

    final File dataFile =
        inputStreamToFile(wrapStream(dataStream, useV4ChunkedWithSigningFormat),
            objectRootFolder.toPath().resolve(DATA_FILE));
    s3Object.setDataFile(dataFile);

    s3Object.setSize(Long.toString(dataFile.length()));

    final BasicFileAttributes attributes =
        Files.readAttributes(dataFile.toPath(), BasicFileAttributes.class);
    s3Object.setCreationDate(
        S3_OBJECT_DATE_FORMAT.format(new Date(attributes.creationTime().toMillis())));
    s3Object.setModificationDate(
        S3_OBJECT_DATE_FORMAT.format(new Date(attributes.lastModifiedTime().toMillis())));
    s3Object.setLastModified(attributes.lastModifiedTime().toMillis());

    s3Object.setMd5(digest(kmsKeyId, dataFile));

    objectMapper.writeValue(new File(objectRootFolder, META_FILE), s3Object);

    return s3Object;
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

    final Bucket theBucket = getBucket(bucketName);

    final File objectRootFolder = createObjectRootFolder(theBucket, s3Object.getName());

    s3Object.setTags(tags);
    objectMapper.writeValue(new File(objectRootFolder, META_FILE), s3Object);
  }

  /**
   * Retrieves a Bucket or creates a new one if not found.
   *
   * @param bucketName The Bucket's Name.
   *
   * @return The Bucket.
   *
   * @throws IOException If Bucket can't be created.
   */
  private Bucket getBucketOrCreateNewOne(final String bucketName) throws IOException {
    Bucket theBucket = getBucket(bucketName);
    if (theBucket == null) {
      theBucket = createBucket(bucketName);
    }
    return theBucket;
  }

  /**
   * Creates the root folder in which to store data and meta file.
   *
   * @param theBucket the Bucket containing the Object.
   * @param objectName name of the object to be stored.
   *
   * @return The Folder to store the Object in.
   */
  private File createObjectRootFolder(final Bucket theBucket, final String objectName) {
    final Path bucketPath = theBucket.getPath();
    final File objectRootFolder = new File(bucketPath.toFile(), objectName);
    objectRootFolder.mkdirs();

    return objectRootFolder;
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
        targetFile.createNewFile();
      }

      outputStream = new FileOutputStream(targetFile);
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
    final Bucket theBucket = getBucket(requireNonNull(bucketName, "bucketName == null"));

    S3Object theObject = null;
    final Path metaPath = theBucket.getPath().resolve(truncateFirstSlash(objectName) + "/" + META_FILE);

    if (Files.exists(metaPath)) {
      try {
        theObject = objectMapper.readValue(metaPath.toFile(), S3Object.class);
        theObject.setDataFile(theBucket.getPath().resolve(truncateFirstSlash(objectName) + "/" + DATA_FILE).toFile());
      } catch (final IOException e) {
        LOG.error("File can not be read", e);
        e.printStackTrace();
      }
    }
    return theObject;
  }
  
  private String truncateFirstSlash(String path) {
    if (path.startsWith("/")) {
      return path.substring(1, path.length());
    } else {
      return path;
    }
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

    final Stream<Path> directoryHierarchy = Files.walk(theBucket.getPath());

    final Set<Path> collect = directoryHierarchy
        .filter(path -> path.toFile().isDirectory())
        .map(path -> theBucket.getPath().relativize(path))
        .filter(path -> isEmpty(prefix)
            || (null != normalizedPrefix
            // match by prefix...
            && path.toString().startsWith(normalizedPrefix)))
        .collect(toSet());

    for (final Path path : collect) {
      final S3Object s3Object = getS3Object(bucketName, path.toString());
      if (s3Object != null) {
        resultObjects.add(s3Object);
      }
    }

    return resultObjects;
  }

  /**
   * Copies an object, identified by bucket and name, to a another bucket and objectName.
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
    final S3Object sourceObject = getS3Object(sourceBucketName, sourceObjectName);

    if (sourceObject == null) {
      return null;
    }
    final S3Object copiedObject =
        putS3Object(destinationBucketName,
            destinationObjectName,
            sourceObject.getContentType(),
            sourceObject.getContentEncoding(),
            new FileInputStream(sourceObject.getDataFile()),
            false,
            sourceObject.getUserMetadata());

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getMd5());
  }

  /**
   * Copies an object, identified by bucket and name, to a another bucket and objectName.
   *
   * @param sourceBucketName name of the bucket to copy from.
   * @param sourceObjectName name of the object to copy.
   * @param destinationBucketName name of the destination bucket.
   * @param destinationObjectName name of the destination object.
   * @param userMetadata User metadata to store for destination object
   *
   * @return an {@link CopyObjectResult} or null if source couldn't be found.
   *
   * @throws FileNotFoundException no FileInputStream of the sourceFile can be created.
   * @throws IOException If File can't be read.
   */
  public CopyObjectResult copyS3Object(final String sourceBucketName,
      final String sourceObjectName,
      final String destinationBucketName,
      final String destinationObjectName,
      final Map<String, String> userMetadata) throws IOException {
    final S3Object sourceObject = getS3Object(sourceBucketName, sourceObjectName);

    if (sourceObject == null) {
      return null;
    }
    final S3Object copiedObject =
        putS3Object(destinationBucketName,
            destinationObjectName,
            sourceObject.getContentType(),
            sourceObject.getContentEncoding(),
            new FileInputStream(sourceObject.getDataFile()),
            false,
            userMetadata);

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getMd5());
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
    final S3Object sourceObject = getS3Object(sourceBucketName, sourceObjectName);
    if (sourceObject == null) {
      return null;
    }
    final S3Object copiedObject =
        putS3ObjectWithKMSEncryption(destinationBucketName,
            destinationObjectName,
            sourceObject.getContentType(),
            new FileInputStream(sourceObject.getDataFile()),
            false,
            sourceObject.getUserMetadata(),
            encryption,
            kmsKeyId);

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getMd5());
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
    final S3Object copiedObject =
        putS3ObjectWithKMSEncryption(destinationBucketName,
            destinationObjectName,
            sourceObject.getContentType(),
            new FileInputStream(sourceObject.getDataFile()),
            false,
            userMetadata,
            encryption,
            kmsKeyId);

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getMd5());
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

    if (!Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, uploadId).toFile()
        .mkdirs()) {
      throw new IllegalStateException(
          "Directories for storing multipart uploads couldn't be created.");
    }
    final MultipartUpload upload =
        new MultipartUpload(fileName, uploadId, owner, initiator, new Date());
    uploadIdToInfo.put(uploadId, new MultipartUploadInfo(upload,
        contentType, contentEncoding, userMetadata));

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
   * Lists the not-yet completed parts of an multipart upload.
   *
   * @return the list of not-yet completed multipart uploads.
   */
  public Collection<MultipartUpload> listMultipartUploads() {
    return uploadIdToInfo.values().stream().map(info -> info.upload).collect(Collectors.toList());
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
          Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, uploadId,
              partNumber + PART_SUFFIX));

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
      final String uploadId, final List<Part> parts) {

    return completeMultipartUpload(bucketName, fileName, uploadId, parts, null, null);
  }

  /**
   * Completes an Multipart Upload for the given Id.
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
      final String uploadId, final List<Part> parts, final String encryption,
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
        s3Object.setCreationDate(S3_OBJECT_DATE_FORMAT.format(
            new Date(attributes.creationTime().toMillis())));
        s3Object.setModificationDate(S3_OBJECT_DATE_FORMAT.format(
            new Date(attributes.lastModifiedTime().toMillis())));
        s3Object.setLastModified(attributes.lastModifiedTime().toMillis());
        s3Object.setMd5(DigestUtils.md5Hex(allMd5s) + "-" + partNames.length);
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
            Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, META_FILE).toFile(),
            s3Object);
      } catch (final IOException e) {
        throw new IllegalStateException("Could not write metadata-file", e);
      }

      return s3Object.getMd5();
    });
  }

  private String[] listAndSortPartsInFromDirectory(final File partFolder) {
    final String[] partNames = partFolder.list((dir, name) -> name.endsWith(PART_SUFFIX));

    Arrays.sort(partNames,
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
    return Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, uploadId).toFile();
  }

  private List<Part> arrangeSeparateParts(final File[] files, final String bucketName,
      final String fileName, final String uploadId) {

    final List<Part> parts = new ArrayList<>();

    for (int i = 0; i < files.length; i++) {
      final String filePartPath = concatUploadIdAndPartFileName(files[i], uploadId);

      final File currentFilePart = retrieveFile(bucketName, fileName, filePartPath);

      final int partNumber = i + 1;
      final String partMd5 = calculateHashOfFilePart(currentFilePart);
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

  private String calculateHashOfFilePart(final File currentFilePart) {
    try (final InputStream is = FileUtils.openInputStream(currentFilePart)) {
      final String partMd5 = DigestUtils.md5Hex(is);
      return String.format("%s", partMd5);
    } catch (final IOException e) {
      LOG.error("Hash could not be calculated. File access did not succeed", e);
      return "";
    }
  }

  private String concatUploadIdAndPartFileName(final File file, final String uploadId) {
    return String.format("%s/%s", uploadId, file.getName());
  }

  private long writeEntireFile(final File entireFile, final File partFolder,
      final String... partNames) {
    try (final OutputStream targetStream = new FileOutputStream(entireFile)) {
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
      return HashUtil.getDigest(salt, inputStream);
    } catch (final NoSuchAlgorithmException e) {
      LOG.error("Hash can not be calculated!", e);
      return null;
    }
  }

  /**
   * Copies the range, define by from/to, from the S3 Object, identified by the given key to given
   * destination into the given bucket.
   *
   * @param bucket The source Bucket.
   * @param key Identifies the S3 Object.
   * @param from Byte range form.
   * @param to Byte range to.
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
      final int from,
      final int to,
      final String partNumber,
      final String destinationBucket,
      final String destinationFilename,
      final String uploadId) throws IOException {

    verifyMultipartUploadPreparation(destinationBucket, destinationFilename, uploadId);

    final File targetPartFile =
        ensurePartFile(partNumber, destinationBucket, destinationFilename, uploadId);

    return copyPart(bucket, key, from, to, targetPartFile);
  }

  private String copyPart(final String bucket,
      final String key,
      final int from,
      final int to,
      final File partFile) throws IOException {
    final int len = to - from + 1;
    final S3Object s3Object = resolveS3Object(bucket, key);

    try (final InputStream sourceStream = FileUtils.openInputStream(s3Object.getDataFile());
        final OutputStream targetStream = new FileOutputStream(partFile)) {
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
    final File partFile = Paths.get(rootFolder.getAbsolutePath(),
        destinationBucket,
        destinationFilename,
        uploadId,
        partNumber + PART_SUFFIX).toFile();

    if (!partFile.exists() && !partFile.createNewFile()) {
      throw new IllegalStateException("Could not create buffer file");
    }
    return partFile;
  }

  private void verifyMultipartUploadPreparation(final String destinationBucket,
      final String destinationFilename, final String uploadId) {
    final Path partsFolder =
        Paths.get(rootFolder.getAbsolutePath(), destinationBucket, destinationFilename, uploadId);

    if (!partsFolder.toFile().exists() || !partsFolder.toFile().isDirectory()) {
      throw new IllegalStateException("Missed preparing Multipart Request");
    }
  }

  private S3Object resolveS3Object(final String bucket, final String key) {
    final S3Object s3Object = getS3Object(bucket, key);

    if (s3Object == null) {
      throw new IllegalStateException("Source Object not found");
    }
    return s3Object;
  }
}
