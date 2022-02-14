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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class BatchDeleteRequestTest {

  @Test
  void testDeserialization(TestInfo testInfo) throws IOException {
    BatchDeleteRequest iut = deserialize(BatchDeleteRequest.class, testInfo);

    assertThat(iut.isQuiet()).isFalse();
    assertThat(iut.getObjectsToDelete()).hasSize(2);

    S3ObjectIdentifier s3ObjectIdentifier0 = iut.getObjectsToDelete().get(0);
    assertThat(s3ObjectIdentifier0.getKey()).isEqualTo("key0");
    assertThat(s3ObjectIdentifier0.getVersionId()).isEqualTo("versionId0");

    S3ObjectIdentifier s3ObjectIdentifier1 = iut.getObjectsToDelete().get(1);
    assertThat(s3ObjectIdentifier1.getKey()).isEqualTo("key1");
    assertThat(s3ObjectIdentifier1.getVersionId()).isEqualTo("versionId1");
  }
}
