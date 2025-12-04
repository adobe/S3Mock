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

package com.adobe.testing.s3mock.store

import com.adobe.testing.s3mock.dto.BucketInfo
import com.adobe.testing.s3mock.dto.BucketLifecycleConfiguration
import com.adobe.testing.s3mock.dto.LocationInfo
import com.adobe.testing.s3mock.dto.ObjectLockConfiguration
import com.adobe.testing.s3mock.dto.ObjectOwnership
import com.adobe.testing.s3mock.dto.VersioningConfiguration
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import java.nio.file.Path
import java.util.UUID

/**
 * Represents a bucket in S3, used to serialize and deserialize all metadata locally.
 */
data class BucketMetadata(
  val name: String,
  val creationDate: String,
  val versioningConfiguration: VersioningConfiguration?,
  val objectLockConfiguration: ObjectLockConfiguration?,
  val bucketLifecycleConfiguration: BucketLifecycleConfiguration?,
  val objectOwnership: ObjectOwnership?,
  val path: Path,
  val bucketRegion: String,
  val bucketInfo: BucketInfo?,
  val locationInfo: LocationInfo?,
  @param:JsonProperty("objects")
  private val _objects: MutableMap<String, UUID> = mutableMapOf()
) {
  val objects: Map<String, UUID>
    get() = java.util.Collections.unmodifiableMap(_objects)

  fun addKey(key: String): UUID {
    val existing = _objects[key]
    if (existing != null) return existing
    val uuid = UUID.randomUUID()
    _objects[key] = uuid
    return uuid
  }

  fun removeKey(key: String): Boolean =
    _objects.remove(key) != null

  fun getID(key: String): UUID? = _objects[key]

  fun withVersioningConfiguration(versioningConfiguration: VersioningConfiguration): BucketMetadata =
    this.copy(versioningConfiguration = versioningConfiguration)

  fun withObjectLockConfiguration(objectLockConfiguration: ObjectLockConfiguration): BucketMetadata =
    this.copy(objectLockConfiguration = objectLockConfiguration)

  fun withBucketLifecycleConfiguration(bucketLifecycleConfiguration: BucketLifecycleConfiguration?): BucketMetadata =
    this.copy(bucketLifecycleConfiguration = bucketLifecycleConfiguration)

  @get:JsonIgnore
  val isVersioningEnabled: Boolean
    get() = this.versioningConfiguration?.status == VersioningConfiguration.Status.ENABLED

  @get:JsonIgnore
  val isVersioningSuspended: Boolean
    get() = this.versioningConfiguration?.status == VersioningConfiguration.Status.SUSPENDED
}
