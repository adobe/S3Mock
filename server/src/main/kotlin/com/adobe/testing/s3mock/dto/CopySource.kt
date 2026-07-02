/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.dto

import software.amazon.awssdk.utils.http.SdkHttpUtils

/**
 * Represents a S3 Object referenced by Bucket and Key.
 */
data class CopySource(
  val bucket: String,
  val key: String,
  val versionId: String?,
) {
  companion object {
    /**
     * Creates a [CopySource] expecting the given String to represent the source as `/{bucket}/{key}[?versionId={versionId}]`.
     * We need to decode here because Spring does not do the decoding for RequestHeaders as it does
     * for path parameters.
     *
     * @param copySource The object references.
     *
     * @throws IllegalArgumentException If `copySource` could not be parsed.
     * @throws NullPointerException If `copySource` is null.
     */
    @JvmStatic
    fun from(copySource: String?): CopySource {
      val decoded = requireNotNull(SdkHttpUtils.urlDecode(copySource)) { "copySource == null" }
      val source = decoded.removePrefix("/")
      require(source.contains("/")) { "Expected a copySource as '/{bucket}/{key}' but got: $copySource" }
      val bucket = source.substringBefore('/')
      val keyWithVersion = source.substringAfter('/')
      val versionId = if (keyWithVersion.contains("?versionId=")) keyWithVersion.substringAfter("?versionId=") else null
      val key = if (versionId != null) keyWithVersion.substringBefore("?versionId=") else keyWithVersion
      return CopySource(bucket, key, versionId)
    }
  }
}
