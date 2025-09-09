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
import java.nio.file.Path
import java.util.UUID

/**
 * Represents a bucket in S3, used to serialize and deserialize all metadata locally.
 */
@JvmRecord
data class BucketMetadata(
    @JvmField val name: String,
    @JvmField val creationDate: String,
    @JvmField val versioningConfiguration: VersioningConfiguration?,
    @JvmField val objectLockConfiguration: ObjectLockConfiguration?,
    @JvmField val bucketLifecycleConfiguration: BucketLifecycleConfiguration?,
    @JvmField val objectOwnership: ObjectOwnership?,
    @JvmField val path: Path,
    @JvmField val bucketRegion: String,
    @JvmField val bucketInfo: BucketInfo?,
    @JvmField val locationInfo: LocationInfo?,
    @JvmField val objects: MutableMap<String, UUID> = mutableMapOf()
) {
    fun withVersioningConfiguration(versioningConfiguration: VersioningConfiguration): BucketMetadata =
      this.copy(versioningConfiguration = versioningConfiguration)

    fun withObjectLockConfiguration(objectLockConfiguration: ObjectLockConfiguration): BucketMetadata =
      this.copy(objectLockConfiguration = objectLockConfiguration)

    fun withBucketLifecycleConfiguration(bucketLifecycleConfiguration: BucketLifecycleConfiguration?): BucketMetadata =
      this.copy(bucketLifecycleConfiguration = bucketLifecycleConfiguration)

    fun doesKeyExist(key: String): Boolean {
        return getID(key) != null
    }

    fun addKey(key: String): UUID {
        if (doesKeyExist(key)) {
            return getID(key)!!
        } else {
            val uuid = UUID.randomUUID()
          this.objects[key] = uuid
          return uuid
        }
    }

    fun removeKey(key: String): Boolean {
        val removed = this.objects.remove(key)
        return removed != null
    }

    fun getID(key: String): UUID? {
        return this.objects[key]
    }

    @get:JsonIgnore
    val isVersioningEnabled: Boolean
        get() = this.versioningConfiguration != null && this.versioningConfiguration.status != null && this.versioningConfiguration.status == VersioningConfiguration.Status.ENABLED

    @get:JsonIgnore
    val isVersioningSuspended: Boolean
        get() = this.versioningConfiguration != null && this.versioningConfiguration.status != null && this.versioningConfiguration.status == VersioningConfiguration.Status.SUSPENDED
}
