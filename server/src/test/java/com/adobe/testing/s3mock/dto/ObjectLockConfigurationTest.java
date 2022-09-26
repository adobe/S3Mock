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

package com.adobe.testing.s3mock.dto;

import static com.adobe.testing.s3mock.dto.DtoTestUtil.deserialize;
import static com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ObjectLockConfigurationTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    DefaultRetention retention = new DefaultRetention(1, null, Mode.COMPLIANCE);
    ObjectLockRule rule = new ObjectLockRule(retention);
    ObjectLockConfiguration iut = new ObjectLockConfiguration(ObjectLockEnabled.ENABLED, rule);

    serializeAndAssert(iut, testInfo);
  }


  @Test
  void testDeserialization(TestInfo testInfo) throws IOException {
    ObjectLockConfiguration iut = deserialize(ObjectLockConfiguration.class, testInfo);
    assertThat(iut.objectLockEnabled()).isNull();
    assertThat(iut.objectLockRule().defaultRetention().years()).isEqualTo(1);
    assertThat(iut.objectLockRule().defaultRetention().mode()).isEqualTo(Mode.COMPLIANCE);
  }
}
