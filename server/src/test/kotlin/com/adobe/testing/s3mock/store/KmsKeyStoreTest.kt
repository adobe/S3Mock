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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.bean.override.mockito.MockitoBean

@MockitoBean(types = [BucketStore::class, ObjectStore::class, MultipartStore::class])
@SpringBootTest(classes = [StoreConfiguration::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
internal class KmsKeyStoreTest {
  @Autowired
  private lateinit var kmsKeyStore: KmsKeyStore

  @Test
  fun `valid registered keyRef makes validateKeyId return true`() {
    val keyId = "valid-test-key-id"
    val keyRef = "arn:aws:kms:us-east-1:1234567890:key/$keyId"
    kmsKeyStore.registerKMSKeyRef(keyRef)
    assertThat(kmsKeyStore.validateKeyId(keyId)).isTrue()
  }
}
