/*
 *  Copyright 2017-2023 Adobe.
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

package com.adobe.testing.s3mock.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.codec.digest.DigestUtils.getMd5Digest;
import static org.apache.commons.codec.digest.DigestUtils.updateDigest;
import static org.apache.commons.io.FileUtils.openInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util-Class for the creation of Digests.
 * These are digests as expected in S3 responses by the AWS SDKs, so they may be generated using
 * algorithms otherwise not expected to be used for this.
 */
public final class DigestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DigestUtil.class);

  private DigestUtil() {
    // private constructor for utility classes
  }

  /**
   * Calculates a hex encoded MD5 digest for the contents of a list of paths.
   * This is a special case that emulates how AWS calculates the MD5 Checksums of the parts of a
   * Multipart upload:
   * <a href="https://stackoverflow.com/questions/12186993/what-is-the-algorithm-to-compute-the-amazon-s3-etag-for-a-file-larger-than-5gb">
   *   Stackoverflow
   * </a>
   * Quote from Stackoverflow:
   *   Say you uploaded a 14MB file to a bucket without server-side encryption, and your part size
   *   is 5MB. Calculate 3 MD5 checksums corresponding to each part, i.e. the checksum of the first
   *   5MB, the second 5MB, and the last 4MB.
   *   Then take the checksum of their concatenation.
   *   MD5 checksums are often printed as hex representations of binary data, so make sure you take
   *   the MD5 of the decoded binary concatenation, not of the ASCII or UTF-8 encoded concatenation.
   *   When that's done, add a hyphen and the number of parts to get the ETag.
   *
   * @param paths the list of paths.
   *
   * @return A special hex digest that is used for files uploaded in parts.
   *
   * @throws IOException if a path could not be accessed.
   */
  public static String hexDigestMultipart(List<Path> paths) throws IOException {
    return DigestUtils.md5Hex(md5(null, paths)) + "-" + paths.size();
  }

  public static String hexDigest(byte[] bytes) {
    return DigestUtils.md5Hex(bytes);
  }

  public static String hexDigest(File file) {
    try (var is = openInputStream(file)) {
      return hexDigest(is);
    } catch (IOException e) {
      LOG.error("Digest could not be calculated.", e);
      throw new IllegalStateException("Digest could not be calculated.", e);
    }
  }

  public static String hexDigest(String salt, File file) {
    try (var is = openInputStream(file)) {
      return hexDigest(salt, is);
    } catch (IOException e) {
      LOG.error("Digest could not be calculated.", e);
      throw new IllegalStateException("Digest could not be calculated.", e);
    }
  }

  /**
   * Calculates a hex encoded MD5 digest for the content of an inputStream.
   *
   * <p>Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a hex encoded MD5 digest as the ETag, as part of the response Header to
   * verify the validity of the transferred file.</p>
   *
   * @param inputStream the InputStream.
   *
   * @return String Hex MD5 digest.
   */
  public static String hexDigest(InputStream inputStream) {
    return hexDigest(null, inputStream);
  }

  /**
   * Calculates a hex encoded MD5 digest for the content of an inputStream.
   *
   * <p>Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a hex encoded MD5 digest as the ETag, as part of the response Header to
   * verify the validity of the transferred file. For encrypted uploads, the returned digest may not
   * be the same as the local client digest value.</p>
   *
   * @param salt Optional salt to add to be digested, for simulating encryption dependent digest.
   * @param inputStream the InputStream.
   *
   * @return String Hex MD5 digest.
   */
  public static String hexDigest(String salt, InputStream inputStream) {
    return Hex.encodeHexString(md5(salt, inputStream));
  }

  /**
   * Calculates a base64 MD5 digest for the content of an inputStream.
   *
   * <p>Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a base64 MD5 digest, ETag, as part of the response Header to verify the
   * validity of the transferred file.</p>
   *
   * @param inputStream the InputStream.
   *
   * @return String Base64 MD5 digest.
   */
  public static String base64Digest(InputStream inputStream) {
    return base64Digest(null, inputStream);
  }

  /**
   * Calculates a base64 MD5 digest for the content of an inputStream.
   *
   * <p>Mainly used for comparison of files. E.g. After Putting a File to the Server, the Amazon
   * S3-Client expects a base64 MD5 digest, ETag, as part of the response Header to verify the
   * validity of the transferred file. For encrypted uploads, the returned digest may not be the
   * same
   * as the local client digest value.</p>
   *
   * @param salt Optional salt to add to be digested, for simulating encryption dependent
   *     digest.
   * @param inputStream the InputStream.
   *
   * @return String Base64 MD5 digest.
   */
  public static String base64Digest(String salt, InputStream inputStream) {
    return Base64.encodeBase64String(md5(salt, inputStream));
  }

  private static byte[] md5(String salt, InputStream inputStream) {
    var messageDigest = messageDigest(salt);
    try {
      return updateDigest(messageDigest, inputStream).digest();
    } catch (IOException e) {
      LOG.error("Could not update digest.", e);
      throw new IllegalStateException("Could not update digest.", e);
    }
  }

  private static byte[] md5(String salt, List<Path> paths) {
    var allMd5s = new byte[0];
    for (var path : paths) {
      try (var inputStream = Files.newInputStream(path)) {
        allMd5s = ArrayUtils.addAll(allMd5s, md5(salt, inputStream));
      } catch (IOException e) {
        LOG.error("Could not read from path {}", path, e);
        throw new IllegalStateException("Could not read from path " + path, e);
      }
    }
    return allMd5s;
  }

  private static MessageDigest messageDigest(String salt) {
    var messageDigest = getMd5Digest();
    messageDigest.reset();

    if (salt != null) {
      updateDigest(messageDigest, salt.getBytes(UTF_8));
    }
    return messageDigest;
  }
}
