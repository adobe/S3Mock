/*
 *  Copyright 2017-2022 Adobe.
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

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;

/**
 * Util-Class for the creation of Digests.
 */
public class DigestUtil {

  /**
   * Calculates a hex encoded MD5 digest for the content of an inputStream.
   *
   * <p>Mainly used for comparison of files. E.g. After PUTting a File to the Server, the Amazon
   * S3-Client expects a hex encoded MD5 digest as the ETag, as part of the response Header to
   * verify the validity of the transferred file.</p>
   *
   * @param inputStream the InputStream.
   *
   * @return String Hex MD5 digest.
   *
   * @throws NoSuchAlgorithmException if no md5 can be found
   * @throws IOException if InputStream can't be read
   */
  public static String getHexDigest(final InputStream inputStream)
      throws NoSuchAlgorithmException, IOException {
    return getHexDigest(null, inputStream);
  }

  /**
   * Calculates a hex encoded MD5 digest for the content of an inputStream.
   *
   * <p>Mainly used for comparison of files. E.g. After PUTting a File to the Server, the Amazon
   * S3-Client expects a hex encoded MD5 digest as the ETag, as part of the response Header to
   * verify the validity of the transferred file. For encrypted uploads, the returned digest may not
   * be the same as the local client digest value.</p>
   *
   * @param salt Optional salt to add to be digested, for simulating encryption dependent digest.
   * @param inputStream the InputStream.
   *
   * @return String Hex MD5 digest.
   *
   * @throws NoSuchAlgorithmException if no md5 can be found.
   * @throws IOException if InputStream can't be read.
   */
  public static String getHexDigest(final String salt, final InputStream inputStream)
      throws NoSuchAlgorithmException, IOException {
    final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
    messageDigest.reset();

    if (salt != null) {
      messageDigest.update(salt.getBytes(UTF_8));
    }

    final byte[] bytes = new byte[1024];
    int numBytes;
    while ((numBytes = inputStream.read(bytes)) != -1) {
      messageDigest.update(bytes, 0, numBytes);
    }
    final byte[] digest = messageDigest.digest();
    return new String(Hex.encodeHex(digest));
  }
}
