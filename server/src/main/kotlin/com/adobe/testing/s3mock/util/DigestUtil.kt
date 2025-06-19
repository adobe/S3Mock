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

package com.adobe.testing.s3mock.util

import com.adobe.testing.s3mock.S3Exception
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.ArrayUtils
import software.amazon.awssdk.checksums.SdkChecksum
import software.amazon.awssdk.utils.BinaryUtils
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

/**
 * Util-Class for the creation of Digests.
 * These are digests as expected in S3 responses by the AWS SDKs, so they may be generated using
 * algorithms otherwise not expected to be used for this.
 */
object DigestUtil {
  private const val DIGEST_COULD_NOT_BE_CALCULATED = "Digest could not be calculated."
  private const val CHECKSUM_COULD_NOT_BE_CALCULATED = "Checksum could not be calculated."

  @JvmStatic
  fun verifyChecksum(
    expected: String,
    actual: String?,
    checksumAlgorithm: ChecksumAlgorithm
  ) {
    if (expected != actual) {
      when (checksumAlgorithm) {
        ChecksumAlgorithm.SHA1 -> throw S3Exception.BAD_CHECKSUM_SHA1
        ChecksumAlgorithm.SHA256 -> throw S3Exception.BAD_CHECKSUM_SHA256
        ChecksumAlgorithm.CRC32 -> throw S3Exception.BAD_CHECKSUM_CRC32
        ChecksumAlgorithm.CRC32C -> throw S3Exception.BAD_CHECKSUM_CRC32C
        ChecksumAlgorithm.CRC64NVME -> throw S3Exception.BAD_CHECKSUM_CRC64NVME
      }
    }
  }

  /**
   * Calculate a checksum for the given path and algorithm.
   *
   * @param path Path containing the bytes to generate the checksum for
   * @param algorithm algorithm to use
   * @return the checksum
   */
  @JvmStatic
  fun checksumFor(path: Path, algorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm): String {
    try {
      Files.newInputStream(path).use {
        return checksumFor(it, algorithm)
      }
    } catch (e: IOException) {
      throw IllegalStateException(CHECKSUM_COULD_NOT_BE_CALCULATED, e)
    }
  }

  /**
   * Calculate a checksum for the given inputstream and algorithm.
   *
   * @param stream InputStream containing the bytes to generate the checksum for
   * @param algorithm algorithm to use
   * @return the checksum
   */
  private fun checksumFor(
      stream: InputStream,
      algorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm
  ): String {
    return BinaryUtils.toBase64(checksum(stream, algorithm))
  }

  /**
   * Calculate a checksum for the given inputstream and algorithm.
   *
   * @param stream InputStream containing the bytes to generate the checksum for
   * @param algorithm algorithm to use
   * @return the checksum
   */
  private fun checksum(
      stream: InputStream,
      algorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm
  ): ByteArray {
    val sdkChecksum = SdkChecksum.forAlgorithm(algorithm)
    try {
      val buffer = ByteArray(4096)
      var read: Int
      while ((stream.read(buffer).also { read = it }) != -1) {
        sdkChecksum.update(buffer, 0, read)
      }
      return sdkChecksum.checksumBytes
    } catch (e: IOException) {
      throw IllegalStateException(CHECKSUM_COULD_NOT_BE_CALCULATED, e)
    }
  }

  private fun checksum(
      paths: List<Path>,
      algorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm
  ): ByteArray {
    val sdkChecksum = SdkChecksum.forAlgorithm(algorithm)
    var allChecksums = ByteArray(0)
    for (path in paths) {
      try {
        Files.newInputStream(path).use {
          allChecksums = ArrayUtils.addAll(allChecksums, *checksum(it, algorithm))
        }
      } catch (e: IOException) {
        throw IllegalStateException("Could not read from path $path", e)
      }
    }
    sdkChecksum.update(allChecksums, 0, allChecksums.size)
    allChecksums = sdkChecksum.checksumBytes
    return allChecksums
  }

  /**
   * Calculates a hex encoded MD5 digest for the contents of a list of paths.
   * This is a special case that emulates how AWS calculates the MD5 Checksums of the parts of a
   * Multipart upload.
   * [API](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
   * [Stackoverflow](https://stackoverflow.com/questions/12186993/what-is-the-algorithm-to-compute-the-amazon-s3-etag-for-a-file-larger-than-5gb)
   * Quote from Stackoverflow:
   * Say you uploaded a 14MB file to a bucket without server-side encryption, and your part size
   * is 5MB. Calculate 3 MD5 checksums corresponding to each part, i.e. the checksum of the first
   * 5MB, the second 5MB, and the last 4MB.
   * Then take the checksum of their concatenation.
   * MD5 checksums are often printed as hex representations of binary data, so make sure you take
   * the MD5 of the decoded binary concatenation, not of the ASCII or UTF-8 encoded concatenation.
   * When that's done, add a hyphen and the number of parts to get the ETag.
   *
   * @param paths the list of paths.
   *
   * @return A special hex digest that is used for files uploaded in parts.
   */
  @JvmStatic
  fun hexDigestMultipart(paths: List<Path>): String {
    return DigestUtils.md5Hex(md5(null, paths)) + "-" + paths.size
  }

  /**
   * Calculates the checksum for a list of paths.
   * For multipart uploads, AWS takes the checksum of all parts, concatenates them, and then takes
   * the checksum again. Then, they add a hyphen and the number of parts used to calculate the
   * checksum.
   * [API](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
   */
  @JvmStatic
  fun checksumMultipart(
      paths: List<Path>,
      algorithm: software.amazon.awssdk.checksums.spi.ChecksumAlgorithm
  ): String {
    return BinaryUtils.toBase64(checksum(paths, algorithm)) + "-" + paths.size
  }

  @JvmStatic
  fun hexDigest(bytes: ByteArray): String {
    return DigestUtils.md5Hex(bytes)
  }

  @JvmStatic
  fun hexDigest(file: File): String {
    try {
      FileUtils.openInputStream(file).use { stream ->
        return hexDigest(stream)
      }
    } catch (e: IOException) {
      throw IllegalStateException(DIGEST_COULD_NOT_BE_CALCULATED, e)
    }
  }

  @JvmStatic
  fun hexDigest(salt: String?, file: File): String {
    try {
      FileUtils.openInputStream(file).use { stream ->
        return hexDigest(salt, stream)
      }
    } catch (e: IOException) {
      throw IllegalStateException(DIGEST_COULD_NOT_BE_CALCULATED, e)
    }
  }

  /**
   * Calculates a hex encoded MD5 digest for the content of an inputStream.
   *
   *
   * Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a hex encoded MD5 digest as the ETag, as part of the response Header to
   * verify the validity of the transferred file.
   *
   * @param inputStream the InputStream.
   *
   * @return String Hex MD5 digest.
   */
  @JvmStatic
  fun hexDigest(inputStream: InputStream): String {
    return hexDigest(null, inputStream)
  }

  /**
   * Calculates a hex encoded MD5 digest for the content of an inputStream.
   *
   *
   * Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a hex encoded MD5 digest as the ETag, as part of the response Header to
   * verify the validity of the transferred file. For encrypted uploads, the returned digest may not
   * be the same as the local client digest value.
   *
   * @param salt Optional salt to add to be digested, for simulating encryption dependent digest.
   * @param inputStream the InputStream.
   *
   * @return String Hex MD5 digest.
   */
  @JvmStatic
  fun hexDigest(salt: String?, inputStream: InputStream): String {
    return Hex.encodeHexString(md5(salt, inputStream))
  }

  /**
   * Calculates a base64 MD5 digest for the content of an inputStream.
   *
   *
   * Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a base64 MD5 digest, ETag, as part of the response Header to verify the
   * validity of the transferred file.
   *
   * @param inputStream the InputStream.
   *
   * @return String Base64 MD5 digest.
   */
  @JvmStatic
  fun base64Digest(inputStream: InputStream): String {
    return base64Digest(null, inputStream)
  }

  /**
   * Calculates a base64 MD5 digest for the content of an inputStream.
   *
   *
   * Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a base64 MD5 digest, ETag, as part of the response Header to verify the
   * validity of the transferred file. For encrypted uploads, the returned digest may not be the
   * same
   * as the local client digest value.
   *
   * @param salt Optional salt to add to be digested, for simulating encryption dependent
   * digest.
   * @param inputStream the InputStream.
   *
   * @return String Base64 MD5 digest.
   */
  private fun base64Digest(salt: String?, inputStream: InputStream): String {
    return Base64.encodeBase64String(md5(salt, inputStream))
  }

  fun base64Digest(binaryData: ByteArray): String {
    return BinaryUtils.toBase64(binaryData)
  }

  private fun md5(salt: String?, inputStream: InputStream): ByteArray {
    val messageDigest = messageDigest(salt)
    try {
      return DigestUtils.updateDigest(messageDigest, inputStream).digest()
    } catch (e: IOException) {
      throw IllegalStateException("Could not update digest.", e)
    }
  }

  private fun md5(salt: String?, paths: List<Path>): ByteArray {
    var allMd5s = ByteArray(0)
    for (path in paths) {
      try {
        Files.newInputStream(path).use { inputStream ->
          allMd5s = ArrayUtils.addAll(allMd5s, *md5(salt, inputStream))
        }
      } catch (e: IOException) {
        throw IllegalStateException("Could not read from path " + path, e)
      }
    }
    return allMd5s
  }

  private fun messageDigest(salt: String?): MessageDigest {
    val messageDigest = DigestUtils.getMd5Digest()
    messageDigest.reset()

    if (salt != null) {
      DigestUtils.updateDigest(messageDigest, salt.toByteArray(StandardCharsets.UTF_8))
    }
    return messageDigest
  }
}
