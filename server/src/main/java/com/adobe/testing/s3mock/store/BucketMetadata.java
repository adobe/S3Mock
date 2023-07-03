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

import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration;
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration;
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
    ObjectLockConfiguration objectLockConfiguration,
    BucketLifecycleConfiguration bucketLifecycleConfiguration,
    Path path,
    Map<String, UUID> objects
) {

  public BucketMetadata(String name, String creationDate,
      ObjectLockConfiguration objectLockConfiguration,
      BucketLifecycleConfiguration bucketLifecycleConfiguration,
      Path path) {
    this(name,
        creationDate,
        objectLockConfiguration,
        bucketLifecycleConfiguration,
        path,
        new HashMap<>());
  }

  public BucketMetadata withObjectLockConfiguration(
      ObjectLockConfiguration objectLockConfiguration) {
    return new BucketMetadata(name(), creationDate(), objectLockConfiguration,
        bucketLifecycleConfiguration(), path());
  }

  public BucketMetadata withBucketLifecycleConfiguration(
      BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    return new BucketMetadata(name(), creationDate(), objectLockConfiguration(),
        bucketLifecycleConfiguration, path());
  }

  public boolean doesKeyExist(String key) {
    return getID(key) != null;
  }

  public UUID addKey(String key) {
    if (doesKeyExist(key)) {
      return getID(key);
    } else {
      UUID uuid = UUID.randomUUID();
      this.objects.put(key, uuid);
      return uuid;
    }
  }

  public boolean removeKey(String key) {
    UUID removed = this.objects.remove(key);
    return removed != null;
  }

  public UUID getID(String key) {
    return this.objects.get(key);
  }
}
