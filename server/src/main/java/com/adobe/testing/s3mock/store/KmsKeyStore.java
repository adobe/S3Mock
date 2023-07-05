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

package com.adobe.testing.s3mock.store;

import static java.util.regex.Pattern.compile;

import com.adobe.testing.s3mock.S3MockApplication;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Stores valid KMS key references for the {@link S3MockApplication}.
 * KMS key references must be added in valid ARN format:
 * "arn:aws:kms:region:acct-id:key/key-id"
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html">API Reference</a>
 */
public record KmsKeyStore(
    Map<String, String> kmsKeysIdToARN
) {

  private static final Pattern VALID_KMS_KEY_ARN =
      compile("arn:aws:kms:([a-zA-Z]+)-([a-zA-Z]+)-(\\d+):(\\d+):key/.*");

  public KmsKeyStore(Set<String> validKmsKeys) {
    this(new ConcurrentHashMap<>());
    validKmsKeys.forEach(this::registerKMSKeyRef);
  }

  /**
   * Register a valid KMS Key reference.
   * KMS key references must be added in valid ARN format:
   * "arn:aws:kms:region:acct-id:key/key-id"
   *
   * @param validKeyRef A KMS Key reference.
   */
  public void registerKMSKeyRef(final String validKeyRef) {
    if (VALID_KMS_KEY_ARN.matcher(validKeyRef).matches()) {
      var kmsKey = validKeyRef.split("/");
      kmsKeysIdToARN.put(kmsKey[1], validKeyRef);
    }
  }

  /**
   * Validate if the KMS key ID is valid.
   *
   * @param keyId A KMS ID reference.
   *
   * @return Returns true if the key ID is valid for this Mock instance.
   */
  public boolean validateKeyId(final String keyId) {
    return kmsKeysIdToARN.containsKey(keyId);
  }
}
