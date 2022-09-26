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

import static com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ListMultipartUploadsResultTest {
  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    ListMultipartUploadsResult iut =
        new ListMultipartUploadsResult("bucketName", "keyMarker", "/", "prefix/", "uploadIdMarker",
            2, false,
            "nextKeyMarker", "nextUploadIdMarker", createMultipartUploads(2),
            List.of(new Prefix("prefix1/"), new Prefix("prefix2/")));

    serializeAndAssert(iut, testInfo);
  }

  private List<MultipartUpload> createMultipartUploads(int count) {
    List<MultipartUpload> multipartUploads = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      MultipartUpload multipartUpload =
          new MultipartUpload("key" + i, "uploadId" + i,
              new Owner(String.valueOf(10L + i), "displayName10" + i),
              new Owner(String.valueOf(100L + i), "displayName100" + i),
              StorageClass.STANDARD,
              new Date(1514477008120L));
      multipartUploads.add(multipartUpload);
    }
    return multipartUploads;
  }
}
