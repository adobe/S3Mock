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

import static com.adobe.testing.s3mock.dto.VersioningConfiguration.Status.ENABLED;
import static com.adobe.testing.s3mock.dto.VersioningConfiguration.Status.SUSPENDED;

import com.adobe.testing.s3mock.dto.BucketInfo;
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.LocationInfo;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
import com.adobe.testing.s3mock.dto.ObjectOwnership;
import com.adobe.testing.s3mock.dto.VersioningConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a bucket in S3, used to serialize and deserialize all metadata locally.
 */
public record BucketMetadata(
    String name,
    String creationDate,
    VersioningConfiguration versioningConfiguration,
    ObjectLockConfiguration objectLockConfiguration,
    BucketLifecycleConfiguration bucketLifecycleConfiguration,
    ObjectOwnership objectOwnership,
    Path path,
    String bucketRegion,
    BucketInfo bucketInfo,
    LocationInfo locationInfo,
    Map<String, UUID> objects
) {

  public BucketMetadata(String name,
      String creationDate,
      VersioningConfiguration versioningConfiguration,
      ObjectLockConfiguration objectLockConfiguration,
      BucketLifecycleConfiguration bucketLifecycleConfiguration,
      ObjectOwnership objectOwnership,
      Path path,
      String bucketRegion,
      BucketInfo bucketInfo,
      LocationInfo locationInfo) {
    this(name,
        creationDate,
        versioningConfiguration,
        objectLockConfiguration,
        bucketLifecycleConfiguration,
        objectOwnership,
        path,
        bucketRegion,
        bucketInfo,
        locationInfo,
        new HashMap<>());
  }

  public BucketMetadata withVersioningConfiguration(
      VersioningConfiguration versioningConfiguration) {
    return new BucketMetadata(name(),
        creationDate(),
        versioningConfiguration,
        objectLockConfiguration(),
        bucketLifecycleConfiguration(),
        objectOwnership(),
        path(),
        bucketRegion(),
        bucketInfo(),
        locationInfo()
    );
  }

  public BucketMetadata withObjectLockConfiguration(
      ObjectLockConfiguration objectLockConfiguration) {
    return new BucketMetadata(name(),
        creationDate(),
        versioningConfiguration(),
        objectLockConfiguration,
        bucketLifecycleConfiguration(),
        objectOwnership(),
        path(),
        bucketRegion(),
        bucketInfo(),
        locationInfo()
    );
  }

  public BucketMetadata withBucketLifecycleConfiguration(
      BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    return new BucketMetadata(name(),
        creationDate(),
        versioningConfiguration(),
        objectLockConfiguration(),
        bucketLifecycleConfiguration,
        objectOwnership(),
        path(),
        bucketRegion(),
        bucketInfo(),
        locationInfo()
    );
  }

  public boolean doesKeyExist(String key) {
    return getID(key) != null;
  }

  public UUID addKey(String key) {
    if (doesKeyExist(key)) {
      return getID(key);
    } else {
      var uuid = UUID.randomUUID();
      this.objects.put(key, uuid);
      return uuid;
    }
  }

  public boolean removeKey(String key) {
    var removed = this.objects.remove(key);
    return removed != null;
  }

  public UUID getID(String key) {
    return this.objects.get(key);
  }

  @JsonIgnore
  public boolean isVersioningEnabled() {
    return this.versioningConfiguration() != null
        && this.versioningConfiguration().status() != null
        && this.versioningConfiguration().status() == ENABLED;
  }

  @JsonIgnore
  public boolean isVersioningSuspended() {
    return this.versioningConfiguration() != null
        && this.versioningConfiguration().status() != null
        && this.versioningConfiguration().status() == SUSPENDED;
  }
}
