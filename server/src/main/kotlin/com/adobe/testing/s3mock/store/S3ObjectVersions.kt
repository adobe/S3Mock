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

import com.fasterxml.jackson.annotation.JsonIgnore
import java.util.UUID

/**
 * [doc](https://docs.aws.amazon.com/AmazonS3/latest/userguide/manage-objects-versioned-bucket.html).
 */
data class S3ObjectVersions(
  val id: UUID,
  val versions: MutableList<String>
) {
  constructor(id: UUID) : this(id, mutableListOf())

  fun createVersion(): String {
    val versionId = UUID.randomUUID().toString()
    versions.add(versionId)
    return versionId
  }

  @get:JsonIgnore
  val latestVersion: String?
    get() {
      if (versions.isEmpty()) {
        return null
      }
      return versions[versions.size - 1]
    }

  fun deleteVersion(versionId: String) {
    versions.remove(versionId)
  }

  companion object {
    fun empty(id: UUID): S3ObjectVersions {
      return S3ObjectVersions(id)
    }
  }
}
