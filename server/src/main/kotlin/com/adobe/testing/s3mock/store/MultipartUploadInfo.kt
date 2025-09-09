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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import com.adobe.testing.s3mock.dto.ChecksumType
import com.adobe.testing.s3mock.dto.MultipartUpload
import com.adobe.testing.s3mock.dto.StorageClass
import com.adobe.testing.s3mock.dto.Tag

/**
 * Encapsulates [MultipartUpload] and corresponding `contentType`.
 */
@JvmRecord
data class MultipartUploadInfo(
  val upload: MultipartUpload,
  val contentType: String?,
  val userMetadata: Map<String, String>,
  val storeHeaders: Map<String, String>,
  @JvmField val encryptionHeaders: Map<String, String>,
  val bucket: String,
  val storageClass: StorageClass?,
  val tags: List<Tag>?,
  @JvmField val checksum: String?,
  val checksumType: ChecksumType?,
  @JvmField val checksumAlgorithm: ChecksumAlgorithm?,
  @JvmField val completed: Boolean
) {
    constructor(
        upload: MultipartUpload,
        contentType: String,
        userMetadata: Map<String, String>,
        storeHeaders: Map<String, String>,
        encryptionHeaders: Map<String, String>,
        bucket: String,
        storageClass: StorageClass,
        tags: List<Tag>,
        checksum: String,
        checksumType: ChecksumType,
        checksumAlgorithm: ChecksumAlgorithm
    ) : this(
        upload,
        contentType,
        userMetadata,
        storeHeaders,
        encryptionHeaders,
        bucket,
        storageClass,
        tags,
        checksum,
        checksumType,
        checksumAlgorithm,
        false
    )

    fun complete(): MultipartUploadInfo {
        return MultipartUploadInfo(
            this.upload,
            this.contentType,
            this.userMetadata,
            this.storeHeaders,
            this.encryptionHeaders,
            this.bucket,
            this.storageClass,
            this.tags,
            this.checksum,
            this.checksumType,
            this.checksumAlgorithm,
            true
        )
    }
}
