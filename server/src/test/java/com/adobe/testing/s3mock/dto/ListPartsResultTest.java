/*
 *  Copyright 2017-2021 Adobe.
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

import static com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ListPartsResultTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    ListPartsResult iut =
        new ListPartsResult("bucketName", "fileName", "uploadId",
            createParts(2));

    serializeAndAssert(iut, testInfo);
  }

  private List<Part> createParts(int count) {
    List<Part> parts = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      Part part = new Part();
      part.setPartNumber(i);
      part.setETag("etag" + i);
      part.setLastModified(new Date(1514477008120L));
      part.setSize(10L + i);
      parts.add(part);
    }
    return parts;
  }
}
