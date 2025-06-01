/*
 *  Copyright 2017-2025 Adobe.
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

import static com.adobe.testing.s3mock.S3Exception.INVALID_COPY_REQUEST_SAME_KEY;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.DigestUtil.hexDigest;
import static java.lang.String.format;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.CanonicalUser;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
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
import org.apache.commons.io.FileUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStore extends StoreBase {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStore.class);
  private static final String META_FILE = "objectMetadata.json";
  private static final String DATA_FILE = "binaryData";
  private static final String VERSIONED_META_FILE = "%s-objectMetadata.json";
  private static final String VERSIONED_DATA_FILE = "%s-binaryData";
  private static final String VERSIONS_FILE = "versions.json";
  //if a bucket isn't version enabled, some APIs return "null" as the versionId for objects.
  //clients may also pass in "null" as a version, expecting the behaviour for non-versioned objects.
  private static final String NULL_VERSION = "null";

  /**
   * This map stores one lock object per S3Object ID.
   * Any method modifying the underlying file must acquire the lock object before the modification.
   */
  private final Map<UUID, Object> lockStore = new ConcurrentHashMap<>();

  private final DateTimeFormatter s3ObjectDateFormat;

  private final ObjectMapper objectMapper;

  public ObjectStore(DateTimeFormatter s3ObjectDateFormat,
      ObjectMapper objectMapper) {
    this.s3ObjectDateFormat = s3ObjectDateFormat;
    this.objectMapper = objectMapper;
  }

  public S3ObjectMetadata storeS3ObjectMetadata(
      BucketMetadata bucket,
      UUID id,
      String key,
      @Nullable String contentType,
      @Nullable Map<String, String> storeHeaders,
      Path path,
      @Nullable Map<String, String> userMetadata,
      @Nullable Map<String, String> encryptionHeaders,
      @Nullable String etag,
      @Nullable List<Tag> tags,
      @Nullable ChecksumAlgorithm checksumAlgorithm,
      @Nullable String checksum,
      Owner owner,
      @Nullable StorageClass storageClass,
      @Nullable ChecksumType checksumType) {
    lockStore.putIfAbsent(id, new Object());
    synchronized (lockStore.get(id)) {
      createObjectRootFolder(bucket, id);
      String versionId = null;
      if (bucket.isVersioningEnabled()) {
        var existingVersions = getS3ObjectVersions(bucket, id);
        if (existingVersions != null) {
          versionId = existingVersions.createVersion();
          writeVersionsfile(bucket, id, existingVersions);
        } else {
          var newVersions = createS3ObjectVersions(bucket, id);
          versionId = newVersions.createVersion();
          writeVersionsfile(bucket, id, newVersions);
        }
      }
      var dataFile = inputPathToFile(path, getDataFilePath(bucket, id, versionId));
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
          checksum,
          storageClass,
          null,
          versionId,
          false,
          checksumType
      );
      writeMetafile(bucket, s3ObjectMetadata);
      return s3ObjectMetadata;
    }
  }

  private AccessControlPolicy privateCannedAcl(Owner owner) {
    var grant = new Grant(new CanonicalUser(owner.displayName(), owner.id()),
        Grant.Permission.FULL_CONTROL);
    return new AccessControlPolicy(owner, Collections.singletonList(grant));
  }

  public void storeObjectTags(BucketMetadata bucket, UUID id, @Nullable String versionId, @Nullable List<Tag> tags) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId);
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
          s3ObjectMetadata.checksum(),
          s3ObjectMetadata.storageClass(),
          s3ObjectMetadata.policy(),
          s3ObjectMetadata.versionId(),
          s3ObjectMetadata.deleteMarker(),
          s3ObjectMetadata.checksumType()
      ));
    }
  }

  public void storeLegalHold(BucketMetadata bucket, UUID id, @Nullable String versionId, LegalHold legalHold) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId);
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
          s3ObjectMetadata.checksum(),
          s3ObjectMetadata.storageClass(),
          s3ObjectMetadata.policy(),
          s3ObjectMetadata.versionId(),
          s3ObjectMetadata.deleteMarker(),
          s3ObjectMetadata.checksumType()
      ));
    }
  }

  public void storeAcl(BucketMetadata bucket, UUID id, @Nullable String versionId, AccessControlPolicy policy) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId);
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
              s3ObjectMetadata.retention(),
              s3ObjectMetadata.owner(),
              s3ObjectMetadata.storeHeaders(),
              s3ObjectMetadata.encryptionHeaders(),
              s3ObjectMetadata.checksumAlgorithm(),
              s3ObjectMetadata.checksum(),
              s3ObjectMetadata.storageClass(),
              policy,
              s3ObjectMetadata.versionId(),
              s3ObjectMetadata.deleteMarker(),
              s3ObjectMetadata.checksumType()
          )
      );
    }
  }

  public AccessControlPolicy readAcl(BucketMetadata bucket, UUID id, @Nullable String versionId) {
    var s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId);
    return s3ObjectMetadata.policy() == null
        ? privateCannedAcl(s3ObjectMetadata.owner())
        : s3ObjectMetadata.policy();
  }

  public void storeRetention(BucketMetadata bucket, UUID id, @Nullable String versionId, Retention retention) {
    synchronized (lockStore.get(id)) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId);
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
          s3ObjectMetadata.checksum(),
          s3ObjectMetadata.storageClass(),
          s3ObjectMetadata.policy(),
          s3ObjectMetadata.versionId(),
          s3ObjectMetadata.deleteMarker(),
          s3ObjectMetadata.checksumType()
      ));
    }
  }

  @Nullable
  public S3ObjectMetadata getS3ObjectMetadata(BucketMetadata bucket, UUID id, @Nullable String versionId) {
    if (bucket.isVersioningEnabled() && versionId == null) {
      var s3ObjectVersions = getS3ObjectVersions(bucket, id);
      versionId = s3ObjectVersions.getLatestVersion();
    }
    var metaPath = getMetaFilePath(bucket, id, versionId);

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

  @Nullable
  public S3ObjectVersions getS3ObjectVersions(BucketMetadata bucket, UUID id) {
    var metaPath = getVersionFilePath(bucket, id);

    if (Files.exists(metaPath)) {
      synchronized (lockStore.get(id)) {
        try {
          return objectMapper.readValue(metaPath.toFile(), S3ObjectVersions.class);
        } catch (IOException e) {
          throw new IllegalArgumentException("Could not read object versions-file " + id, e);
        }
      }
    }
    return null;
  }

  @Nullable
  public S3ObjectVersions createS3ObjectVersions(BucketMetadata bucket, UUID id) {
    var metaPath = getVersionFilePath(bucket, id);

    if (Files.exists(metaPath)) {
      //gracefully handle duplicate version creation
      return getS3ObjectVersions(bucket, id);
    } else {
      synchronized (lockStore.get(id)) {
        try {
          writeVersionsfile(bucket, id, new S3ObjectVersions(id));
          return objectMapper.readValue(metaPath.toFile(), S3ObjectVersions.class);
        } catch (java.io.IOException e) {
          throw new IllegalArgumentException("Could not read object versions-file " + id, e);
        }
      }
    }
  }

  @Nullable
  public S3ObjectMetadata copyS3Object(
      BucketMetadata sourceBucket,
      UUID sourceId,
      @Nullable String versionId,
      BucketMetadata destinationBucket,
      UUID destinationId,
      String destinationKey,
      @Nullable Map<String, String> encryptionHeaders,
      @Nullable Map<String, String> storeHeaders,
      @Nullable Map<String, String> userMetadata,
      @Nullable StorageClass storageClass) {
    var sourceObject = getS3ObjectMetadata(sourceBucket, sourceId, versionId);
    if (sourceObject == null) {
      return null;
    }
    synchronized (lockStore.get(sourceId)) {
      return storeS3ObjectMetadata(destinationBucket,
          destinationId,
          destinationKey,
          sourceObject.contentType(),
          storeHeaders == null || storeHeaders.isEmpty()
              ? sourceObject.storeHeaders() : storeHeaders,
          sourceObject.dataPath(),
          userMetadata == null || userMetadata.isEmpty()
              ? sourceObject.userMetadata() : userMetadata,
          encryptionHeaders == null || encryptionHeaders.isEmpty()
              ? sourceObject.encryptionHeaders() : encryptionHeaders,
          null,
          sourceObject.tags(),
          sourceObject.checksumAlgorithm(),
          sourceObject.checksum(),
          sourceObject.owner(),
          storageClass != null ? storageClass : sourceObject.storageClass(),
          sourceObject.checksumType()
      );
    }
  }

  /**
   * If source and destination is the same, pretend we copied - S3 does the same.
   * This does not change the modificationDate.
   * Also, this would need to increment the version if/when we support versioning.
   */
  @Nullable
  public S3ObjectMetadata pretendToCopyS3Object(
      BucketMetadata sourceBucket,
      UUID sourceId,
      @Nullable String versionId,
      @Nullable Map<String, String> encryptionHeaders,
      @Nullable Map<String, String> storeHeaders,
      @Nullable Map<String, String> userMetadata,
      @Nullable StorageClass storageClass) {
    var sourceObject = getS3ObjectMetadata(sourceBucket, sourceId, versionId);
    if (sourceObject == null) {
      return null;
    }

    verifyPretendCopy(sourceObject, userMetadata, encryptionHeaders, storeHeaders, storageClass);

    var s3ObjectMetadata = new S3ObjectMetadata(
        sourceObject.id(),
        sourceObject.key(),
        sourceObject.size(),
        sourceObject.modificationDate(),
        sourceObject.etag(),
        sourceObject.contentType(),
        java.time.Instant.now().toEpochMilli(),
        sourceObject.dataPath(),
        userMetadata == null || userMetadata.isEmpty()
            ? sourceObject.userMetadata() : userMetadata,
        sourceObject.tags(),
        sourceObject.legalHold(),
        sourceObject.retention(),
        sourceObject.owner(),
        storeHeaders == null || storeHeaders.isEmpty()
            ? sourceObject.storeHeaders() : storeHeaders,
        encryptionHeaders == null || encryptionHeaders.isEmpty()
            ? sourceObject.encryptionHeaders() : encryptionHeaders,
        sourceObject.checksumAlgorithm(),
        sourceObject.checksum(),
        storageClass != null ? storageClass : sourceObject.storageClass(),
        sourceObject.policy(),
        sourceObject.versionId(),
        sourceObject.deleteMarker(),
        sourceObject.checksumType()
    );
    writeMetafile(sourceBucket, s3ObjectMetadata);
    return s3ObjectMetadata;
  }

  private void verifyPretendCopy(
      S3ObjectMetadata sourceObject,
      @Nullable Map<String, String> userMetadata,
      @Nullable Map<String, String> encryptionHeaders,
      @Nullable Map<String, String> storeHeaders,
      @Nullable StorageClass storageClass) {
    var userDataUnChanged = userMetadata == null || userMetadata.isEmpty();
    var encryptionHeadersUnChanged = encryptionHeaders == null || encryptionHeaders.isEmpty();
    var storeHeadersUnChanged = storeHeaders == null || storeHeaders.isEmpty();
    var storageClassUnChanged = storageClass == null || storageClass == sourceObject.storageClass();
    if (userDataUnChanged
        && storageClassUnChanged
        && encryptionHeadersUnChanged
        && storeHeadersUnChanged) {
      throw INVALID_COPY_REQUEST_SAME_KEY;
    }
  }

  public boolean deleteObject(
      BucketMetadata bucket,
      UUID id,
      @Nullable String versionId) {
    var s3ObjectMetadata = getS3ObjectMetadata(bucket, id, versionId);
    if (s3ObjectMetadata != null) {
      if (bucket.isVersioningEnabled() && !"null".equals(versionId)) {
        if (versionId != null) {
          return doDeleteVersion(bucket, id, versionId);
        } else {
          return insertDeleteMarker(bucket, id, s3ObjectMetadata);
        }
      }
      return doDeleteObject(bucket, id);
    } else {
      return false;
    }
  }

  private boolean doDeleteVersion(BucketMetadata bucket, UUID id, String versionId) {
    synchronized (lockStore.get(id)) {
      try {
        var existingVersions = getS3ObjectVersions(bucket, id);
        if (existingVersions.versions().size() <= 1) {
          //this is the last version of an object, delete object completely.
          return doDeleteObject(bucket, id);
        } else {
          //there is at least one version of an object left, delete only the version.
          existingVersions.deleteVersion(versionId);
          writeVersionsfile(bucket, id, existingVersions);
        }
      } catch (Exception e) {
        throw new IllegalStateException("Could not delete object-version " + id, e);
      }
      return false;
    }
  }

  public boolean doDeleteObject(BucketMetadata bucket, UUID id) {
    synchronized (lockStore.get(id)) {
      try {
        FileUtils.deleteDirectory(getObjectFolderPath(bucket, id).toFile());
      } catch (IOException e) {
        throw new IllegalStateException("Could not delete object-directory " + id, e);
      }
      lockStore.remove(id);
      return true;
    }
  }

  /**
   * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html">API Reference</a>.
   */
  private boolean insertDeleteMarker(
      BucketMetadata bucket,
      UUID id,
      S3ObjectMetadata s3ObjectMetadata) {
    String versionId = null;
    synchronized (lockStore.get(id)) {
      try {
        var existingVersions = getS3ObjectVersions(bucket, id);
        if (existingVersions != null) {
          versionId = existingVersions.createVersion();
          writeVersionsfile(bucket, id, existingVersions);
        }
        writeMetafile(bucket, S3ObjectMetadata.deleteMarker(s3ObjectMetadata, versionId));
      } catch (Exception e) {
        throw new IllegalStateException("Could not insert object-deletemarker " + id, e);
      }
      return false;
    }
  }

  /**
   * Used to load metadata for all objects from a bucket when S3Mock starts.
   * @param bucketMetadata metadata of existing bucket.
   * @param ids ids of the keys to load
   */
  void loadObjects(BucketMetadata bucketMetadata, Collection<UUID> ids) {
    var loaded = 0;
    for (var id : ids) {
      lockStore.putIfAbsent(id, new Object());
      var s3ObjectVersions = getS3ObjectVersions(bucketMetadata, id);
      if (s3ObjectVersions != null) {
        if (loadVersions(bucketMetadata, s3ObjectVersions)) {
          loaded++;
        }
      } else {
        var s3ObjectMetadata = getS3ObjectMetadata(bucketMetadata, id, null);
        if (s3ObjectMetadata != null) {
          loaded++;
        }
      }
    }
    LOG.info("Loaded {}/{} objects for bucket {}", loaded, ids.size(), bucketMetadata.name());
  }

  private boolean loadVersions(BucketMetadata bucket, S3ObjectVersions versions) {
    var loaded = false;
    var s3ObjectVersions = getS3ObjectVersions(bucket, versions.id());
    for (var version : s3ObjectVersions.versions()) {
      var s3ObjectMetadata = getS3ObjectMetadata(bucket, versions.id(), version);
      if (s3ObjectMetadata != null) {
        loaded = true;
      }
    }
    return loaded;
  }

  private void createObjectRootFolder(BucketMetadata bucket, UUID id) {
    var objectRootFolder = getObjectFolderPath(bucket, id).toFile();
    objectRootFolder.mkdirs();
  }

  private Path getObjectFolderPath(BucketMetadata bucket, UUID id) {
    return Paths.get(bucket.path().toString(), id.toString());
  }

  private Path getMetaFilePath(BucketMetadata bucket, UUID id, @Nullable String versionId) {
    if (versionId != null && !NULL_VERSION.equals(versionId)) {
      return getObjectFolderPath(bucket, id).resolve(format(VERSIONED_META_FILE, versionId));
    }
    return getObjectFolderPath(bucket, id).resolve(META_FILE);
  }

  private Path getDataFilePath(BucketMetadata bucket, UUID id, @Nullable String versionId) {
    if (versionId != null) {
      return getObjectFolderPath(bucket, id).resolve(format(VERSIONED_DATA_FILE, versionId));
    }
    return getObjectFolderPath(bucket, id).resolve(DATA_FILE);
  }

  private Path getVersionFilePath(BucketMetadata bucket, UUID id) {
    return getObjectFolderPath(bucket, id).resolve(VERSIONS_FILE);
  }

  private void writeVersionsfile(BucketMetadata bucket, UUID id,
                                 S3ObjectVersions s3ObjectVersions) {
    try {
      synchronized (lockStore.get(id)) {
        var versionsFile = getVersionFilePath(bucket, id).toFile();
        objectMapper.writeValue(versionsFile, s3ObjectVersions);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write object versions-file " + id, e);
    }
  }

  private void writeMetafile(BucketMetadata bucket, S3ObjectMetadata s3ObjectMetadata) {
    var id = s3ObjectMetadata.id();
    try {
      synchronized (lockStore.get(id)) {
        var metaFile = getMetaFilePath(bucket, id, s3ObjectMetadata.versionId()).toFile();
        objectMapper.writeValue(metaFile, s3ObjectMetadata);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write object metadata-file " + id, e);
    }
  }
}
