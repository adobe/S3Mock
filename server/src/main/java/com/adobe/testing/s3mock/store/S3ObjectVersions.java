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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/manage-objects-versioned-bucket.html">doc</a>.
 */
public record S3ObjectVersions(
    UUID id,
    Map<Integer, String> versions,
    AtomicInteger latestVersionPointer
) {

  public S3ObjectVersions(UUID id) {
    this(id, new HashMap<>(), new AtomicInteger(0));
  }

  public String createVersion() {
    var versionId = UUID.randomUUID().toString();
    versions.put(latestVersionPointer.getAndIncrement(), versionId);
    return versionId;
  }

  public String getLatestVersion() {
    return versions.get(latestVersionPointer.get() - 1);
  }
}
