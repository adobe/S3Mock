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
package com.adobe.testing.s3mock.its

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.ArrayUtils
import java.io.BufferedInputStream
import java.io.IOException
import java.io.InputStream

/**
 * Etag calculator. Calculates the etag while reading from the input. Only works for buffer reads
 * with ((1024 * 1014) % len) == 0 .
 */
class EtagInputStream(inputStream: InputStream, private val partSize: Long) : BufferedInputStream(inputStream) {
    private var currentPartSize: Long = 0
    private var partCount = 0
    private var allMd5s = ByteArray(0)
    private var currentDigest = DigestUtils.getMd5Digest()
    private var closed = false

    /**
     * Returns calculated etag. Must not be called
     *
     * @return etag.
     */
    val etag: String
        get() {
            check(closed) { "etag should not be called if stream is not closed yet." }
            if (currentPartSize > 0) {
                allMd5s = ArrayUtils.addAll(allMd5s, *currentDigest.digest())
                partCount++
            }
            return DigestUtils.md5Hex(allMd5s) + "-" + partCount
        }

    @Synchronized
    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        check(_1M % len == 0) { "buffer length must be valid for ((1024*1024*1024) % len) == 0" }
        val read = super.read(b, off, len)
        currentPartSize += read.toLong()
        currentDigest.update(b)
        if (currentPartSize >= partSize) {
            allMd5s = ArrayUtils.addAll(allMd5s, *currentDigest.digest())
            currentDigest = DigestUtils.getMd5Digest()
            currentPartSize = 0
            partCount++
        }
        return read
    }

    @Throws(IOException::class)
    override fun close() {
        super.close()
        closed = true
    }

    companion object {
        private const val _1M = 1024 * 1024
    }
}
