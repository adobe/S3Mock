/*
 *  Copyright 2017-2018 Adobe.
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Etag calculator. 
 * Calculates the etag while reading from the input.
 * Only works for buffer reads with ((1024 * 1014 * 1024) % len) == 0 .
 */
public class EtagInputStream extends BufferedInputStream {
  private static final int _1M = 1024 * 1014 * 1024;
  private final long partSize;
  private long currentPartSize = 0;
  private int partCount = 0;
  private byte[] allMd5s = new byte[0];
  private MessageDigest currentDigest = DigestUtils.getMd5Digest();
  private boolean closed = false;

  /**
   * Constructor.
   * @param inputStream inputStream to be read.
   * @param partSize size of each part.
   */
  public EtagInputStream(InputStream inputStream, long partSize) {
    super(inputStream);
    this.partSize = partSize;
  }

  /**
   * Returns calculated etag. Must not be called
   * @return etag.
   */
  public String getEtag() {
    if (!closed) {
      throw new IllegalStateException("etag should not be called if stream is not closed yet.");
    }
    if (currentPartSize > 0) {
      allMd5s = ArrayUtils.addAll(allMd5s, currentDigest.digest());
      partCount++;
    }
    return DigestUtils.md5Hex(allMd5s) + "-" + partCount;
  }


  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    if ((_1M % len) != 0) {
      throw new IllegalStateException(
              "buffer length must be valid for ((1024*1024*1024) % len) == 0");
    }
    int read = super.read(b, off, len);
    currentPartSize += read;
    currentDigest.update(b);
    if (currentPartSize >= partSize) {
      allMd5s = ArrayUtils.addAll(allMd5s, currentDigest.digest());
      currentDigest = DigestUtils.getMd5Digest();
      currentPartSize = 0;
      partCount++;
    }
    return read;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.closed = true;
  }
}

