/*
 *  Copyright 2017 Adobe.
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

import static java.util.Objects.requireNonNull;

import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.util.AwsChunkDecodingInputStream;
import com.adobe.testing.s3mock.util.HashUtil;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.log4j.Logger;
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
  private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

  private static final Logger LOG = Logger.getLogger(FileStore.class);
  private static final int ZERO_OFFSET = 0;

  private final File rootFolder;

  private final Gson gson;

  private final Map<String, String> uploadIdToContentType = new ConcurrentHashMap<>();

  /**
   *
   * Constructs a new {@link FileStore}.
   *
   * @throws IOException due to problems during the rootBucket creation
   */
  public FileStore() throws IOException {
    this(new File(FileUtils.getTempDirectory(), "s3mockFileStore" + new Date().getTime()));
  }

  FileStore(final File rootFolder) {
    this.rootFolder = rootFolder;
    rootFolder.deleteOnExit();
    rootFolder.mkdir();
    gson = new Gson();
  }

  /**
   * Creates a new bucket
   *
   * @param bucketName name of the bucket.
   * @return the newly created Bucket
   * @throws IOException if the bucket cannot be created or the bucket already exists but is not
   * a directory
   */
  public Bucket createBucket(final String bucketName) throws IOException {
    final File newBucket = new File(rootFolder, bucketName);
    FileUtils.forceMkdir(newBucket);

    return bucketFromPath(newBucket.toPath());
  }

  /**
   * Lists all buckets managed by this FileStore
   *
   * @return List of all Buckets
   */
  public List<Bucket> listBuckets() {
    final DirectoryStream.Filter<Path> filter = file -> (Files.isDirectory(file));

    return findBucketsByFilter(filter);
  }

  /**
   * Retrieves a bucket identified by its name.
   *
   * @param bucketName name of the bucket to be retrieved
   * @return the Bucket or null if not found
   */
  public Bucket getBucket(final String bucketName) {
    final DirectoryStream.Filter<Path> filter =
        file -> (Files.isDirectory(file) && file.getFileName().endsWith(bucketName));

    final List<Bucket> buckets = findBucketsByFilter(filter);
    return buckets.size() > 0 ? buckets.get(0) : null;
  }

  /**
   * Searches for folders in the rootFolder that match the given {@link DirectoryStream.Filter}
   *
   * @param filter the Filter to apply
   * @return List of found Folders
   */
  private List<Bucket> findBucketsByFilter(final DirectoryStream.Filter<Path> filter) {
    final List<Bucket> buckets = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootFolder.toPath(), filter)) {
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
   * Stores a File inside a Bucket
   *
   * @param bucketName Bucket to store the File in
   * @param fileName name of the File to be stored
   * @param contentType The files Content Type
   * @param dataStream The File as InputStream
   * @param useV4Signing If {@code true}, V4-style signing is enabled.
   * @return {@link S3Object}
   * @throws IOException if an I/O error occurs
   */
  public S3Object putS3Object(final String bucketName,
      final String fileName,
      final String contentType,
      final InputStream dataStream,
      final boolean useV4Signing) throws IOException {
    final S3Object s3Object = new S3Object();
    s3Object.setName(fileName);
    s3Object.setContentType(contentType);

    final Bucket theBucket = getBucketOrCreateNewOne(bucketName);

    final File objectRootFolder = createObjectRootFolder(theBucket, s3Object.getName());

    final File dataFile =
        inputStreamToFile(wrapStream(dataStream, useV4Signing),
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

    final File metaFile = new File(objectRootFolder, META_FILE);
    final PrintWriter writer = new PrintWriter(metaFile);
    writer.print(gson.toJson(s3Object));
    writer.close();

    return s3Object;
  }

  private InputStream wrapStream(final InputStream dataStream, final boolean useV4Signing) {
    final InputStream inStream;
    if (useV4Signing) {
      inStream = new AwsChunkDecodingInputStream(dataStream);
    } else {
      inStream = dataStream;
    }
    return inStream;
  }

  /**
   * Stores an encrypted File inside a Bucket
   *
   * @param bucketName Bucket to store the File in
   * @param fileName name of the File to be stored
   * @param contentType The files Content Type
   * @param dataStream The File as InputStream
   * @param useV4Signing If {@code true}, V4-style signing is enabled.
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   * @return {@link S3Object}
   * @throws IOException if an I/O error occurs
   */
  public S3Object putS3ObjectWithKMSEncryption(final String bucketName,
      final String fileName,
      final String contentType,
      final InputStream dataStream,
      final boolean useV4Signing,
      final String encryption, final String kmsKeyId) throws IOException {
    final S3Object s3Object = new S3Object();
    s3Object.setName(fileName);
    s3Object.setContentType(contentType);
    s3Object.setEncrypted(true);
    s3Object.setKmsEncryption(encryption);
    s3Object.setKmsEncryptionKeyId(kmsKeyId);

    final Bucket theBucket = getBucketOrCreateNewOne(bucketName);

    final File objectRootFolder = createObjectRootFolder(theBucket, s3Object.getName());

    final File dataFile =
        inputStreamToFile(wrapStream(dataStream, useV4Signing),
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

    final File metaFile = new File(objectRootFolder, META_FILE);

    try (PrintWriter writer = new PrintWriter(metaFile)) {
      writer.print(gson.toJson(s3Object));
    }

    return s3Object;
  }

  /**
   * Retrieves a Bucket or creates a new one if not found
   *
   * @param bucketName The Bucket's Name
   * @return The Bucket
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
   * Creates the root folder in which to store data and meta file
   *
   * @param theBucket the Bucket containing the Object
   * @param objectName name of the object to be stored
   * @return The Folder to store the Object in
   */
  private File createObjectRootFolder(final Bucket theBucket, final String objectName) {
    final Path bucketPath = theBucket.getPath();
    final File objectRootFolder = new File(bucketPath.toFile(), objectName);
    objectRootFolder.mkdirs();

    return objectRootFolder;
  }

  /**
   * Stores the Content of an InputStream in a File Creates File if it not exists
   *
   * @param inputStream the Stream to be saved
   * @param filePath Path where the stream should be saved.
   * @return the newly created File
   */
  private File inputStreamToFile(final InputStream inputStream, final Path filePath) {
    OutputStream outputStream = null;
    final File targetFile = filePath.toFile();
    try {
      if (!targetFile.exists()) {
        targetFile.createNewFile();
      }

      outputStream = new FileOutputStream(targetFile);
      int read = 0;
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
   * Retrieves an Object from a bucket.
   *
   * @param bucketName the Bucket in which to look the file in.
   * @param objectName name of the object.
   * @return the retrieved S3Object or null if not found
   */
  public S3Object getS3Object(final String bucketName, final String objectName) {
    final Bucket theBucket = getBucket(requireNonNull(bucketName, "bucketName == null"));

    S3Object theObject = null;
    final Path metaPath = theBucket.getPath().resolve(objectName + "/" + META_FILE);

    if (Files.exists(metaPath)) {
      final byte[] metaBytes;
      try {
        metaBytes = Files.readAllBytes(metaPath);
        theObject = gson.fromJson(new String(metaBytes, Charset.defaultCharset()), S3Object.class);
        theObject.setDataFile(theBucket.getPath().resolve(objectName + "/" + DATA_FILE).toFile());
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
   * @return the retrieved {@code List<S3Object>} or null if not found
   * @throws IOException if directory stream fails
   */
  public List<S3Object> getS3Objects(final String bucketName, final String prefix)
      throws IOException {
    final Bucket theBucket = getBucket(requireNonNull(bucketName, "bucketName == null"));
    final List<S3Object> theObjects = new ArrayList<>();

    final DirectoryStream<Path> directoryContents =
        Files.newDirectoryStream(theBucket.getPath(),
            entry -> prefix == null || entry.toFile().getName().startsWith(prefix));

    for (final Path path : directoryContents) {
      theObjects.add(getS3Object(bucketName, path.toFile().getName()));
    }

    return theObjects;
  }

  /**
   * Copies an object, identified by bucket and name, to a another bucket and objectName
   *
   * @param sourceBucketName name of the bucket to copy from
   * @param sourceObjectName name of the object to copy
   * @param destinationBucketName name of the destination bucket
   * @param destinationObjectName name of the destination object
   * @return an {@link CopyObjectResult} or null if source couldn't be found
   * @throws FileNotFoundException no FileInputStream of the sourceFile can be created
   * @throws IOException If File can't be read
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
            new FileInputStream(sourceObject.getDataFile()),
            false);

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getMd5());
  }

  /**
   * Copies an object to another bucket and encrypted object
   *
   * @param sourceBucketName name of the bucket to copy from
   * @param sourceObjectName name of the object to copy
   * @param destinationBucketName name of the destination bucket
   * @param destinationObjectName name of the destination object
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   * @return an {@link CopyObjectResult} or null if source couldn't be found
   * @throws FileNotFoundException no FileInputStream of the sourceFile can be created
   * @throws IOException If File can't be read
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
            encryption,
            kmsKeyId);

    return new CopyObjectResult(copiedObject.getModificationDate(), copiedObject.getMd5());
  }

  /**
   * Checks if the specified bucket exists. Amazon S3 buckets are named in a global namespace;
   * use this method to
   * determine if a specified bucket name already exists, and therefore can't be used to create a
   * new bucket.
   *
   * @param bucketName Name of the bucket to check for existence
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
   * @return true if deletion succeeded.
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
   * Deletes a Bucket and all of its contents
   *
   * @param bucketName name of the bucket to be deleted
   * @return true if deletion succeeded
   * @throws IOException if bucket-file could not be accessed
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
   * @param uploadId id of the upload
   */
  public void prepareMultipartUpload(final String bucketName, final String fileName,
      final String contentType, final String uploadId) {

    if (!Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, uploadId).toFile()
        .mkdirs()) {
      throw new IllegalStateException(
          "Directories for storing multipart uploads couldn't be created.");
    }
    uploadIdToContentType.put(uploadId, contentType);
  }

  /**
   * Uploads a part of a multipart upload.
   *
   * @param bucketName in which to upload
   * @param fileName of the file to upload
   * @param uploadId id of the upload
   * @param partNumber number of the part to store
   * @param inputStream file data to be stored
   * @param useV4Signing If {@code true}, V4-style signing is enabled.
   * @return the md5 hash of this part
   * @throws IOException if file could not be read to calculate digest
   */
  public String putPart(final String bucketName,
      final String fileName,
      final String uploadId,
      final String partNumber,
      final InputStream inputStream,
      final boolean useV4Signing) throws IOException {
    try (DigestInputStream digestingInputStream =
        new DigestInputStream(wrapStream(inputStream, useV4Signing),
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
   * @param bucketName to which was uploaded
   * @param fileName which was uploaded
   * @param uploadId of the upload
   * @return the etag of the complete file.
   */
  public String completeMultipartUpload(final String bucketName, final String fileName,
      final String uploadId) {

    return completeMultipartUpload(bucketName, fileName, uploadId, null, null);
  }

  public String completeMultipartUpload(final String bucketName, final String fileName,
      final String uploadId, final String encryption, final String kmsKeyId) {
    final S3Object s3Object = new S3Object();
    s3Object.setName(fileName);

    s3Object.setEncrypted(encryption != null || kmsKeyId != null);
    if (encryption != null) {
      s3Object.setKmsEncryption(encryption);
    }

    if (kmsKeyId != null) {
      s3Object.setKmsEncryptionKeyId(kmsKeyId);
    }

    final File partFolder =
        Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, uploadId).toFile();
    final File entireFile =
        Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, DATA_FILE).toFile();

    final String[] partNames = partFolder.list((dir, name) -> name.endsWith(PART_SUFFIX));
    Arrays.sort(partNames);

    try (DigestOutputStream targetStream =
        new DigestOutputStream(new FileOutputStream(entireFile),
            MessageDigest.getInstance("MD5"))) {
      int size = 0;
      for (final String partName : partNames) {
        size += Files.copy(Paths.get(partFolder.getAbsolutePath(), partName), targetStream);
      }

      FileUtils.deleteDirectory(partFolder);

      final BasicFileAttributes attributes =
          Files.readAttributes(entireFile.toPath(), BasicFileAttributes.class);
      s3Object.setCreationDate(
          S3_OBJECT_DATE_FORMAT.format(new Date(attributes.creationTime().toMillis())));
      s3Object
          .setModificationDate(
              S3_OBJECT_DATE_FORMAT.format(new Date(attributes.lastModifiedTime().toMillis())));
      s3Object.setLastModified(attributes.lastModifiedTime().toMillis());
      s3Object.setMd5(new String(Hex.encodeHex(targetStream.getMessageDigest().digest())) + "-1");
      s3Object.setSize(Integer.toString(size));

      s3Object.setContentType(uploadIdToContentType.getOrDefault(uploadId, DEFAULT_CONTENT_TYPE));

      uploadIdToContentType.remove(uploadId);

    } catch (IOException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Error finishing multipart upload", e);
    }

    try (PrintWriter writer =
        new PrintWriter(
            Paths.get(rootFolder.getAbsolutePath(), bucketName, fileName, META_FILE).toFile())) {
      writer.print(gson.toJson(s3Object));

    } catch (final FileNotFoundException e) {
      throw new IllegalStateException("Could not write metadata-file", e);
    }

    return s3Object.getMd5();
  }

  private String digest(final String salt, final File dataFile) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(dataFile)) {
      return HashUtil.getDigest(salt, inputStream);
    } catch (final NoSuchAlgorithmException e) {
      LOG.error("Hash can not be calculated!", e);
      return null;
    }
  }

  public String copyPart(final String bucket,
      final String key,
      final int from,
      final int to,
      final boolean useV4Signing,
      final String partNumber,
      final String destinationBucket,
      final String destinationFilename,
      final String uploadId) throws IOException {

    verifyMultipartUploadPreparation(destinationBucket, destinationFilename, uploadId);

    final File targetPartFile =
        ensurePartFile(partNumber, destinationBucket, destinationFilename, uploadId);

    copyPart(bucket, key, from, to, useV4Signing, targetPartFile);

    return UUID.randomUUID().toString();
  }

  private void copyPart(final String bucket,
      final String key,
      final int from,
      final int to,
      final boolean useV4Signing,
      final File partFile) throws IOException {
    final int len = to - from + 1;
    final S3Object s3Object = resolveS3Object(bucket, key);

    try (InputStream sourceStream =
        wrapStream(FileUtils.openInputStream(s3Object.getDataFile()), useV4Signing);
        OutputStream targetStream = new FileOutputStream(partFile)) {
      sourceStream.skip(from);
      IOUtils.copy(new BoundedInputStream(sourceStream, len), targetStream);
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

    if (!partFile.exists()) {
      if (!partFile.createNewFile()) {
        throw new IllegalStateException("Could not create buffer file");
      }
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
