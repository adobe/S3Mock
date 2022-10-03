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
import static com.adobe.testing.s3mock.util.XmlUtil.deserializeJaxb;
import static com.adobe.testing.s3mock.util.XmlUtil.serializeJaxb;
import static java.nio.file.Files.newOutputStream;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Grantee;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores objects and their metadata created in S3Mock.
 */
public class ObjectStore {
  private static final String META_FILE = "objectMetadata.json";
  private static final String ACL_FILE = "objectAcl.xml";
  private static final String DATA_FILE = "binaryData";

  private static final Logger LOG = LoggerFactory.getLogger(ObjectStore.class);
  /**
   * This map stores one lock object per S3Object ID.
   * Any method modifying the underlying file must aquire the lock object before the modification.
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
   * @param contentEncoding The Content Encoding.
   * @param dataStream The InputStream to store.
   * @param useV4ChunkedWithSigningFormat If {@code true}, V4-style signing is enabled.
   * @param userMetadata User metadata to store for this object, will be available for the
   *     object with the key prefixed with "x-amz-meta-".
   * @param encryption The Encryption Type.
   * @param kmsKeyId The KMS encryption key id.
   * @param etag the etag. If null, etag will be computed by this method.
   * @param tags The tags to store.
   *
   * @return {@link S3ObjectMetadata}.
   */
  public S3ObjectMetadata storeS3ObjectMetadata(BucketMetadata bucket,
      UUID id,
      String key,
      String contentType,
      String contentEncoding,
      InputStream dataStream,
      boolean useV4ChunkedWithSigningFormat,
      Map<String, String> userMetadata,
      String encryption,
      String kmsKeyId,
      String etag,
      List<Tag> tags,
      Owner owner) {
    Instant now = Instant.now();
    boolean encrypted = isNotBlank(encryption) && isNotBlank(kmsKeyId);
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setId(id);
    s3ObjectMetadata.setKey(key);
    s3ObjectMetadata.setContentType(contentType);
    s3ObjectMetadata.setContentEncoding(contentEncoding);
    s3ObjectMetadata.setUserMetadata(userMetadata);
    s3ObjectMetadata.setTags(tags);
    s3ObjectMetadata.setEncrypted(encrypted);
    s3ObjectMetadata.setKmsEncryption(encryption);
    s3ObjectMetadata.setKmsKeyId(kmsKeyId);
    s3ObjectMetadata.setModificationDate(s3ObjectDateFormat.format(now));
    s3ObjectMetadata.setLastModified(now.toEpochMilli());
    s3ObjectMetadata.setOwner(owner);
    lockStore.putIfAbsent(id, new Object());
    synchronized (lockStore.get(id)) {
      createObjectRootFolder(bucket, id);
      File dataFile =
          inputStreamToFile(wrapStream(dataStream, useV4ChunkedWithSigningFormat),
              getDataFilePath(bucket, id));
      s3ObjectMetadata.setDataPath(dataFile.toPath());
      s3ObjectMetadata.setSize(Long.toString(dataFile.length()));
      s3ObjectMetadata.setEtag(etag != null ? etag : hexDigest(kmsKeyId, dataFile));

      writeMetafile(bucket, s3ObjectMetadata);
    }

    return s3ObjectMetadata;
  }

  private AccessControlPolicy privateCannedAcl(Owner owner) {
    Grant grant = new Grant(Grantee.from(owner), Grant.Permission.FULL_CONTROL);
    return new AccessControlPolicy(owner, Collections.singletonList(grant));
  }

  /**
   * Store tags for a given object.
   *
   * @param bucket Bucket the object is stored in.
   * @param id object ID to store tags for.
   * @param tags List of tag objects.
   */
  public void storeObjectTags(BucketMetadata bucket, UUID id, List<Tag> tags) {
    synchronized (lockStore.get(id)) {
      S3ObjectMetadata s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      s3ObjectMetadata.setTags(tags);
      writeMetafile(bucket, s3ObjectMetadata);
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
      S3ObjectMetadata s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      s3ObjectMetadata.setLegalHold(legalHold);
      writeMetafile(bucket, s3ObjectMetadata);
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
    AccessControlPolicy policy = readAclFile(bucket, id);
    if (policy == null) {
      S3ObjectMetadata s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      return privateCannedAcl(s3ObjectMetadata.getOwner());
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
      S3ObjectMetadata s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
      s3ObjectMetadata.setRetention(retention);
      writeMetafile(bucket, s3ObjectMetadata);
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
    S3ObjectMetadata theObject = null;

    Path metaPath = getMetaFilePath(bucket, id);

    if (Files.exists(metaPath)) {
      synchronized (lockStore.get(id)) {
        try {
          theObject = objectMapper.readValue(metaPath.toFile(), S3ObjectMetadata.class);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read object metadata-file " + id, e);
        }
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
   * @return {@link CopyObjectResult} or null if source couldn't be found.
   */
  public CopyObjectResult copyS3Object(BucketMetadata sourceBucket,
      UUID sourceId,
      BucketMetadata destinationBucket,
      UUID destinationId,
      String destinationKey,
      String encryption,
      String kmsKeyId,
      Map<String, String> userMetadata) {
    S3ObjectMetadata sourceObject = getS3ObjectMetadata(sourceBucket, sourceId);
    if (sourceObject == null) {
      return null;
    }
    S3ObjectMetadata copiedObject;
    synchronized (lockStore.get(sourceId)) {
      try (InputStream inputStream = Files.newInputStream(sourceObject.getDataPath())) {
        copiedObject = storeS3ObjectMetadata(destinationBucket,
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
            null,
            sourceObject.getTags(),
            sourceObject.getOwner());
      } catch (IOException e) {
        LOG.error("Can't write file to disk!", e);
        throw new IllegalStateException("Can't write file to disk!", e);
      }
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
    S3ObjectMetadata sourceObject = getS3ObjectMetadata(sourceBucket, sourceId);
    if (sourceObject == null) {
      return null;
    }

    sourceObject.setLastModified(Instant.now().toEpochMilli());
    sourceObject.setUserMetadata(userMetadata == null || userMetadata.isEmpty()
        ? sourceObject.getUserMetadata() : userMetadata);
    writeMetafile(sourceBucket, sourceObject);
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
    S3ObjectMetadata s3ObjectMetadata = getS3ObjectMetadata(bucket, id);
    if (s3ObjectMetadata != null) {
      synchronized (lockStore.get(id)) {
        try {
          FileUtils.deleteDirectory(getObjectFolderPath(bucket, id).toFile());
        } catch (IOException e) {
          LOG.error("Can't delete directory.", e);
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
    for (UUID id : ids) {
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
      LOG.error("Can't write file to disk!", e);
      throw new IllegalStateException("Can't write file to disk!", e);
    }
    return targetFile;
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

  private Path getAclFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), ACL_FILE);
  }

  //TODO: should be private
  Path getDataFilePath(BucketMetadata bucket, UUID id) {
    return Paths.get(getObjectFolderPath(bucket, id).toString(), DATA_FILE);
  }

  private boolean writeMetafile(BucketMetadata bucket, S3ObjectMetadata s3ObjectMetadata) {
    try {
      synchronized (lockStore.get(s3ObjectMetadata.getId())) {
        File metaFile = getMetaFilePath(bucket, s3ObjectMetadata.getId()).toFile();
        if (!retainFilesOnExit) {
          metaFile.deleteOnExit();
        }
        objectMapper.writeValue(metaFile, s3ObjectMetadata);
        return true;
      }
    } catch (IOException e) {
      LOG.error("Could not write object metadata-file.", e);
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }

  private AccessControlPolicy readAclFile(BucketMetadata bucket, UUID id) {
    try {
      synchronized (lockStore.get(id)) {
        File aclFile = getAclFilePath(bucket, id).toFile();
        if (!aclFile.exists()) {
          return null;
        }
        String toDeserialize = FileUtils.readFileToString(aclFile, Charset.defaultCharset());
        return deserializeJaxb(toDeserialize);
      }
    } catch (IOException | JAXBException | XMLStreamException e) {
      LOG.error("Could not write object metadata-file.", e);
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }

  private boolean writeAclFile(BucketMetadata bucket, UUID id, AccessControlPolicy policy) {
    try {
      synchronized (lockStore.get(id)) {
        File aclFile = getAclFilePath(bucket, id).toFile();
        if (!retainFilesOnExit) {
          aclFile.deleteOnExit();
        }
        FileUtils.write(aclFile, serializeJaxb(policy), Charset.defaultCharset());
        return true;
      }
    } catch (IOException | JAXBException e) {
      LOG.error("Could not write object metadata-file.", e);
      throw new IllegalStateException("Could not write object metadata-file.", e);
    }
  }
}
