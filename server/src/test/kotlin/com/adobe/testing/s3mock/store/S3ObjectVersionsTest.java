/*
 *  Copyright 2017-2024 Adobe.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class S3ObjectVersionsTest {

  @Test
  void testVersions() {
    var iut = new S3ObjectVersions(UUID.randomUUID());
    assertThat(iut.getLatestVersion()).isNull();
    assertThat(iut.latestVersionPointer().get()).isZero();

    var version = iut.createVersion();
    assertThat(version).isNotBlank();
    assertThat(iut.latestVersionPointer().get()).isOne();
    assertThat(iut.getLatestVersion()).isEqualTo(version);
  }
}
