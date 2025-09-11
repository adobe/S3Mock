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
package com.adobe.testing.s3mock.dto

import software.amazon.awssdk.utils.http.SdkHttpUtils
import java.util.Objects

/**
 * Represents a S3 Object referenced by Bucket and Key.
 */
data class CopySource(
  val bucket: String,
  val key: String,
  val versionId: String?
) {
  companion object {
    const val DELIMITER: String = "/"

    /**
     * Creates a [CopySource] expecting the given String to represent the source as `/{bucket}/{key}[?versionId={versionId}]`.
     *
     * @param copySource The object references.
     *
     * @throws IllegalArgumentException If `copySource` could not be parsed.
     * @throws NullPointerException If `copySource` is null.
     */
    @JvmStatic
    fun from(copySource: String?): CopySource {
      val bucketAndKey: Array<String?> = extractBucketAndKeyArray(SdkHttpUtils.urlDecode(copySource))
      val bucket = Objects.requireNonNull<String>(bucketAndKey[0])
      var key = Objects.requireNonNull<String?>(bucketAndKey[1])
      var versionId: String? = null
      if (key!!.contains("?versionId=")) {
        val keyAndVersionId: Array<String?> =
          key.split("\\?versionId=".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        key = keyAndVersionId[0]
        versionId = keyAndVersionId[1]
      }
      return CopySource(bucket, key, versionId)
    }

    /**
     * we need to decode here because Spring does not do the decoding for RequestHeaders as it does
     * for path parameters.
     */
    private fun extractBucketAndKeyArray(copySource: String?): Array<String?> {
      Objects.requireNonNull<String?>(copySource, "copySource == null")
      val source: String = Companion.normalizeCopySource(copySource!!)
      val bucketAndKey: Array<String?> = source.split(DELIMITER.toRegex(), limit = 2).toTypedArray()

      require(bucketAndKey.size == 2) { "Expected a copySource as '/{bucket}/{key}' but got: $copySource" }

      return bucketAndKey
    }

    private fun normalizeCopySource(copySource: String): String {
      if (copySource.startsWith("/")) {
        return copySource.substring(1)
      }
      return copySource
    }
  }
}
