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
package com.adobe.testing.s3mock.vectors

import org.springframework.http.HttpStatus

/**
 * RuntimeException to communicate S3 Vectors errors.
 * Handled by [com.adobe.testing.s3mock.vectors.controller.VectorsControllerConfiguration.S3VectorsExceptionHandler],
 * mapped to a JSON error body with `__type` and `message` fields.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_S3_Vectors.html)
 */
class S3VectorsException(
  val status: Int,
  val type: String,
  override val message: String,
) : RuntimeException(message) {
  companion object {
    fun notFound(message: String): S3VectorsException = S3VectorsException(HttpStatus.NOT_FOUND.value(), "NotFoundException", message)

    fun conflict(message: String): S3VectorsException = S3VectorsException(HttpStatus.CONFLICT.value(), "ConflictException", message)

    fun validation(message: String): S3VectorsException = S3VectorsException(HttpStatus.BAD_REQUEST.value(), "ValidationException", message)

    val VECTOR_BUCKET_NOT_FOUND: S3VectorsException =
      notFound("The specified vector bucket does not exist.")

    val VECTOR_BUCKET_ALREADY_EXISTS: S3VectorsException =
      conflict("The specified vector bucket already exists.")

    val VECTOR_BUCKET_NOT_EMPTY: S3VectorsException =
      conflict("The vector bucket you tried to delete still has indexes.")

    val INDEX_NOT_FOUND: S3VectorsException =
      notFound("The specified index does not exist.")

    val INDEX_ALREADY_EXISTS: S3VectorsException =
      conflict("The specified index already exists.")

    val VECTOR_NOT_FOUND: S3VectorsException =
      notFound("The specified vector does not exist.")

    val INVALID_BUCKET_NAME: S3VectorsException =
      validation("The specified vector bucket name is not valid.")

    val INVALID_INDEX_NAME: S3VectorsException =
      validation("The specified index name is not valid.")

    val INVALID_DIMENSION: S3VectorsException =
      validation("The dimension must be between 1 and 4096.")

    val INVALID_TOP_K: S3VectorsException =
      validation("topK must be between 1 and 100.")

    val INVALID_VECTOR_DATA: S3VectorsException =
      validation("Vector data is invalid (NaN, Inf, or dimension mismatch).")

    val ZERO_VECTOR_COSINE: S3VectorsException =
      validation("Cosine distance requires non-zero vectors.")

    val BUCKET_POLICY_NOT_FOUND: S3VectorsException =
      notFound("The specified vector bucket policy does not exist.")

    val INVALID_FILTER_NON_FILTERABLE_KEY: S3VectorsException =
      validation("Filter references a non-filterable metadata key.")

    fun dimensionMismatch(
      expected: Int,
      actual: Int,
    ): S3VectorsException = validation("Vector dimension $actual does not match index dimension $expected.")
  }
}
