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

class CompleteMultipartUploadTest {

  @Test
  void testDeserialization(TestInfo testInfo) throws IOException {
    CompleteMultipartUpload iut =
        deserialize(CompleteMultipartUpload.class, testInfo);
    assertThat(iut.parts()).hasSize(1);
    CompletedPart part0 = iut.parts().get(0);
    assertThat(part0.etag()).isEqualTo("\"etag\"");
    assertThat(part0.partNumber()).isEqualTo(1);
  }
}
