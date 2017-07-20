/*
 *  Copyright 2017 Adobe.
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

package com.adobe.testing.s3mock.domain;

import com.adobe.testing.s3mock.S3MockApplication;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;

/**
 * Stores valid KMS key references for the {@link S3MockApplication}.
 */
public class KMSKeyStore {
  @Value("${validKmsKeys:}")
  private final Set<String> defaultKeys = new HashSet<>();

  private final Map<String, String> kmsKeys = new ConcurrentHashMap<>();

  /**
   * Register a valid KMS Key reference.
   *
   * @param validKeyRef A KMS Key reference.
   */
  public void registerKMSKeyRef(final String validKeyRef) {
    kmsKeys.put(validKeyRef, validKeyRef);
  }

  /**
   * Validate if the KMS key reference is valid.
   *
   * @param keyRef A KMS Key reference.
   * @return Returns true if the key is valid for this Mock instance.
   */
  public boolean validateKeyRef(final String keyRef) {
    return kmsKeys.containsKey(keyRef);
  }

  @PostConstruct
  private void addDefaultKeys() {
    defaultKeys.forEach(this::registerKMSKeyRef);
  }
}
