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

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import java.util.regex.Pattern

/**
 * Stores valid KMS key references for the [S3MockApplication].
 * KMS key references must be added in valid ARN format:
 * "arn:aws:kms:region:acct-id:key/key-id"
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)
 */
open class KmsKeyStore(val kmsKeysIdToARN: MutableMap<String, String>) {
    constructor(validKmsKeys: Set<String>) : this(ConcurrentHashMap<String, String>()) {
        validKmsKeys.forEach(Consumer { validKeyRef: String? -> this.registerKMSKeyRef(validKeyRef!!) })
    }

    /**
     * Register a valid KMS Key reference.
     * KMS key references must be added in valid ARN format:
     * "arn:aws:kms:region:acct-id:key/key-id"
     *
     * @param validKeyRef A KMS Key reference.
     */
    fun registerKMSKeyRef(validKeyRef: String) {
        if (VALID_KMS_KEY_ARN.matcher(validKeyRef).matches()) {
            val kmsKey: Array<String?> = validKeyRef.split("/".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
          kmsKeysIdToARN[kmsKey[1]!!] = validKeyRef
        }
    }

    /**
     * Validate if the KMS key ID is valid.
     *
     * @param keyId A KMS ID reference.
     *
     * @return Returns true if the key ID is valid for this Mock instance.
     */
    fun validateKeyId(keyId: String): Boolean {
        return kmsKeysIdToARN.containsKey(keyId)
    }

    companion object {
        private val VALID_KMS_KEY_ARN: Pattern =
            Pattern.compile("arn:aws:kms:([a-zA-Z]+)-([a-zA-Z]+)-(\\d+):(\\d+):key/.*")
    }
}
