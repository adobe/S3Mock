/*
 *  Copyright 2017-2023 Adobe.
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
import static com.adobe.testing.s3mock.util.XmlUtil.deserializeJaxb;
import static com.adobe.testing.s3mock.util.XmlUtil.serializeJaxb;
import static java.nio.file.Files.newOutputStream;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Grantee;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.util.AwsChecksumInputStream;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingChecksumInputStream;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.FileUtils;

/**
 * Stores objects and their metadata created in S3Mock.
 */
public class ObjectStore {
  private static final String META_FILE = "objectMetadata.json";
  private static final String ACL_FILE = "objectAcl.xml";
  private static final String DATA_FILE = "binaryData";

  /**
   * This map stores one lock object per S3Object ID.
   * Any method modifying the underlying file must acquire the lock object before the modification.
   */
  private final Map<UUID, Object> lockStore = new ConcurrentHashMap<>();

  private final boolean retainFilesOnExit;
  private final DateTimeFormatter s3ObjectDateFormat;

  private final ObjectMapper objectMapper;

  public ObjectStore(boolean retainFilesOnExit,
      DateTimeFormatter s3ObjectDateFormat, ObjectMapper objectMapper) {
    this.retainFilesOnExit = retainFilesOnExit;
    this.s3ObjectDateFormat = s3ObjectDateFormat;
    this.objectMapper = objectMapper;
  }

  /**
   * Stores an object inside a Bucket.
   *
   * @param bucket Bucket to store the object in.
   * @param id object ID
   * @param key object key to be stored.
   * @param contentType The Content Type.
   * @param storeHeaders Various headers to store, like Content Encoding.
   * @param dataStream The InputStream to store.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
   * @param etag the etag. If null, etag will be computed by this method.
   * @param tags The tags to store.
   *
   * @return {@link S3ObjectMetadata}.
   */
  public S3ObjectMetadata storeS3ObjectMetadata(BucketMetadata bucket,
      UUID id,
      String key,
      String contentType,
      Map<String, String> storeHeaders,
      InputStream dataStream,
      boolean useV4ChunkedWithSigningFormat,
      Map<String, String> userMetadata,
      Map<String, String> encryptionHeaders,
      String etag,
      List<Tag> tags,
      ChecksumAlgorithm checksumAlgorithm,
      String checksum,
      Owner owner) {
    lockStore.putIfAbsent(id, new Object());
    synchronized (lockStore.get(id)) {
      createObjectRootFolder(bucket, id);
      var checksumEmbedded = checksumAlgorithm != null && checksum == null;
      var inputStream = wrapStream(dataStream, useV4ChunkedWithSigningFormat, checksumEmbedded);
      var dataFile = inputStreamToFile(inputStream, getDataFilePath(bucket, id));
      if (inputStream instanceof AwsChecksumInputStream) {
        checksum = ((AwsChecksumInputStream) inputStream).getChecksum();
      }
      var now = Instant.now();
      var s3ObjectMetadata = new S3ObjectMetadata(
          id,
          key,
          Long.toString(dataFile.length()),
          s3ObjectDateFormat.format(now),
          etag != null
              ? etag
              : hexDigest(encryptionHeaders.get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID),
                  dataFile),
          contentType,
          now.toEpochMilli(),
          dataFile.toPath(),
          userMetadata,
          tags,
          null,
          null,
          owner,
          storeHeaders,
          encryptionHeaders,
          checksumAlgorithm,
          checksum
      );
      writeMetafile(bucket, s3ObjectMetadata);
      return s3ObjectMetadata;
    }
  }

  private AccessControlPolicy privateCannedAcl(Owner owner) {
    var grant = new Grant(Grantee.from(owner), Grant.Permission.FULL_CONTROL);
    return new AccessControlPolicy(owner, Collections.singletonList(grant));
  }

  /**
   * Store tags for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param id object ID to store tags for.
   * @param tags List of tagSet objects.
   */
  public void storeObjectTags(BucketMetadata bucket, UUID id, List<Tag> tags) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      writeMetafile(bucket, new S3ObjectMetadata(
          s3ObjectMetadata.id(),
          s3ObjectMetadata.key(),
          s3ObjectMetadata.size(),
          s3ObjectMetadata.modificationDate(),
          s3ObjectMetadata.etag(),
          s3ObjectMetadata.contentType(),
          s3ObjectMetadata.lastModified(),
          s3ObjectMetadata.dataPath(),
          s3ObjectMetadata.userMetadata(),
          tags,
          s3ObjectMetadata.legalHold(),
          s3ObjectMetadata.retention(),
          s3ObjectMetadata.owner(),
          s3ObjectMetadata.storeHeaders(),
          s3ObjectMetadata.encryptionHeaders(),
          s3ObjectMetadata.checksumAlgorithm(),
          s3ObjectMetadata.checksum()
      ));
    }
  }

  /**
   * Store legal hold for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param id object ID to store tags for.
   * @param legalHold the legal hold.
   */
  public void storeLegalHold(BucketMetadata bucket, UUID id, LegalHold legalHold) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      writeMetafile(bucket, new S3ObjectMetadata(
          s3ObjectMetadata.id(),
          s3ObjectMetadata.key(),
          s3ObjectMetadata.size(),
          s3ObjectMetadata.modificationDate(),
          s3ObjectMetadata.etag(),
          s3ObjectMetadata.contentType(),
          s3ObjectMetadata.lastModified(),
          s3ObjectMetadata.dataPath(),
          s3ObjectMetadata.userMetadata(),
          s3ObjectMetadata.tags(),
          legalHold,
          s3ObjectMetadata.retention(),
          s3ObjectMetadata.owner(),
          s3ObjectMetadata.storeHeaders(),
          s3ObjectMetadata.encryptionHeaders(),
          s3ObjectMetadata.checksumAlgorithm(),
          s3ObjectMetadata.checksum()
      ));
    }
  }

  /**
   * Store ACL for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param id object ID to store tags for.
   * @param policy the ACL.
   */
  public void storeAcl(BucketMetadata bucket, UUID id, AccessControlPolicy policy) {
    writeAclFile(bucket, id, policy);
  }

  public AccessControlPolicy readAcl(BucketMetadata bucket, UUID id) {
    var policy = readAclFile(bucket, id);
    if (policy == null) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      return privateCannedAcl(s3ObjectMetadata.owner());
    }
    return policy;
  }

  /**
   * Store retention for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param id object ID to store tags for.
   * @param retention the retention.
   */
  public void storeRetention(BucketMetadata bucket, UUID id, Retention retention) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      writeMetafile(bucket, new S3ObjectMetadata(
          s3ObjectMetadata.id(),
          s3ObjectMetadata.key(),
          s3ObjectMetadata.size(),
          s3ObjectMetadata.modificationDate(),
          s3ObjectMetadata.etag(),
          s3ObjectMetadata.contentType(),
          s3ObjectMetadata.lastModified(),
          s3ObjectMetadata.dataPath(),
          s3ObjectMetadata.userMetadata(),
          s3ObjectMetadata.tags(),
          s3ObjectMetadata.legalHold(),
          retention,
          s3ObjectMetadata.owner(),
          s3ObjectMetadata.storeHeaders(),
          s3ObjectMetadata.encryptionHeaders(),
          s3ObjectMetadata.checksumAlgorithm(),
          s3ObjectMetadata.checksum()
      ));
    }
  }

  /**
   * Retrieves S3ObjectMetadata for a UUID of a key from a bucket.
   *
   * @param bucket Bucket from which to retrieve the object.
   * @param id ID of the object key.
   *
   * @return S3ObjectMetadata or null if not found
   */
  public S3ObjectMetadata getS3ObjectMetadata(BucketMetadata bucket, UUID id) {
    var metaPath = getMetaFilePath(bucket, id);

    if (Files.exists(metaPath)) {
      synchronized (lockStore.get(id)) {
        try {
          return objectMapper.readValue(metaPath.toFile(), S3ObjectMetadata.class);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read object metadata-file " + id, e);
        }
      }
    }
    return null;
  }

  /**
   * Copies an object to another bucket and encrypted object.
   *
   * @param sourceBucket bucket to copy from.
   * @param sourceId source object ID.
   * @param destinationBucket destination bucket.
   * @param destinationId destination object ID.
   * @param destinationKey destination object key.
   * @param userMetadata User metadata to store for destination object
   *
   * @return {@link CopyObjectResult} or null if source couldn't be found.
   */
  public CopyObjectResult copyS3Object(BucketMetadata sourceBucket,
      UUID sourceId,
      BucketMetadata destinationBucket,
      UUID destinationId,
      String destinationKey,
      Map<String, String> encryptionHeaders,
      Map<String, String> userMetadata) {
    var sourceObject = getS3ObjectMetadata(sourceBucket, sourceId);
    if (sourceObject == null) {
      return null;
    }
    synchronized (lockStore.get(sourceId)) {
      try (var inputStream = Files.newInputStream(sourceObject.dataPath())) {
        var copiedObject = storeS3ObjectMetadata(destinationBucket,
            destinationId,
            destinationKey,
            sourceObject.contentType(),
            sourceObject.storeHeaders(),
            inputStream,
            false,
            userMetadata == null || userMetadata.isEmpty()
                ? sourceObject.userMetadata() : userMetadata,
            encryptionHeaders,
            null,
            sourceObject.tags(),
            sourceObject.checksumAlgorithm(),
            sourceObject.checksum(),
            sourceObject.owner());
        return new CopyObjectResult(copiedObject.modificationDate(), copiedObject.etag());
      } catch (IOException e) {
        throw new IllegalStateException("Can't write file to disk!", e);
      }
    }
  }

  /**
   * If source and destination is the same, pretend we copied - S3 does the same.
   * This does not change the modificationDate.
   * Also, this would need to increment the version if/when we support versioning.
   */
  public CopyObjectResult pretendToCopyS3Object(BucketMetadata sourceBucket,
      UUID sourceId,
      Map<String, String> userMetadata) {
    var sourceObject = getS3ObjectMetadata(sourceBucket, sourceId);
    if (sourceObject == null) {
      return null;
    }

    writeMetafile(sourceBucket, new S3ObjectMetadata(
        sourceObject.id(),
        sourceObject.key(),
        sourceObject.size(),
        sourceObject.modificationDate(),
        sourceObject.etag(),
        sourceObject.contentType(),
        Instant.now().toEpochMilli(),
        sourceObject.dataPath(),
        userMetadata == null || userMetadata.isEmpty()
            ? sourceObject.userMetadata() : userMetadata,
        sourceObject.tags(),
        sourceObject.legalHold(),
        sourceObject.retention(),
        sourceObject.owner(),
        sourceObject.storeHeaders(),
        sourceObject.encryptionHeaders(),
        sourceObject.checksumAlgorithm(),
        sourceObject.checksum()
    ));
    return new CopyObjectResult(sourceObject.modificationDate(), sourceObject.etag());
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
    var s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
    if (s3ObjectMetadata != null) {
      synchronized (lockStore.get(id)) {
        try {
          FileUtils.deleteDirectory(getObjectFolderPath(bucket, id).toFile());
        } catch (IOException e) {
          throw new IllegalStateException("Can't delete directory.", e);
        }
        lockStore.remove(id);
        return true;
      }
    } else {
      return false;
    }
  }

  void loadObjects(BucketMetadata bucketMetadata, Collection<UUID> ids) {
    for (var id : ids) {
      lockStore.putIfAbsent(id, new Object());
      getS3ObjectMetadata(bucketMetadata, id);
    }
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
    var targetFile = filePath.toFile();
    try {
      if (targetFile.createNewFile() && (!retainFilesOnExit)) {
        targetFile.deleteOnExit();
      }

      try (var is = inputStream;
          var os = newOutputStream(targetFile.toPath())) {
        int read;
        byte[] bytes = new byte[1024];

        while ((read = is.read(bytes)) != -1) {
          os.write(bytes, 0, read);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Can't write file to disk!", e);
    }
    return targetFile;
  }

  InputStream wrapStream(InputStream dataStream, boolean useV4ChunkedWithSigningFormat,
                         boolean checksumEbedded) {
    if (useV4ChunkedWithSigningFormat && checksumEbedded) {
      return new AwsChunkedDecodingChecksumInputStream(dataStream);
    } else if (useV4ChunkedWithSigningFormat) {
      return new AwsChunkedDecodingInputStream(dataStream);
    } else if (checksumEbedded) {
      return new AwsChecksumInputStream(dataStream);
    } else {
      return dataStream;
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
    var objectRootFolder = getObjectFolderPath(bucket, id).toFile();
    return objectRootFolder.mkdirs();
  }

  private Path getObjectFolderPath(BucketMetadata bucket, UUID id) {
    return Paths.get(bucket.path().toString(), id.toString());
  }

  private Path getMetaFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), META_FILE);
  }

  private Path getAclFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), ACL_FILE);
  }

  //TODO: should be private
  Path getDataFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), DATA_FILE);
  }

  private boolean writeMetafile(BucketMetadata bucket, S3ObjectMetadata s3ObjectMetadata) {
    try {
      synchronized (lockStore.get(s3ObjectMetadata.id())) {
        var metaFile = getMetaFilePath(bucket, s3ObjectMetadata.id()).toFile();
        if (!retainFilesOnExit) {
          metaFile.deleteOnExit();
        }
        objectMapper.writeValue(metaFile, s3ObjectMetadata);
        return true;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }

  private AccessControlPolicy readAclFile(BucketMetadata bucket, UUID id) {
    try {
      synchronized (lockStore.get(id)) {
        var aclFile = getAclFilePath(bucket, id).toFile();
        if (!aclFile.exists()) {
          return null;
        }
        var toDeserialize = FileUtils.readFileToString(aclFile, Charset.defaultCharset());
        return deserializeJaxb(toDeserialize);
      }
    } catch (IOException | JAXBException | XMLStreamException e) {
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }

  private boolean writeAclFile(BucketMetadata bucket, UUID id, AccessControlPolicy policy) {
    try {
      synchronized (lockStore.get(id)) {
        var aclFile = getAclFilePath(bucket, id).toFile();
        if (!retainFilesOnExit) {
          aclFile.deleteOnExit();
        }
        FileUtils.write(aclFile, serializeJaxb(policy), Charset.defaultCharset());
        return true;
      }
    } catch (IOException | JAXBException e) {
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }
}
