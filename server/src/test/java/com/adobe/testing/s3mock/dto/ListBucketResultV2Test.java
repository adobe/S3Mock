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
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ListBucketResultV2Test {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    ListBucketResultV2 iut =
        new ListBucketResultV2("bucketName", "prefix/", 1000, false, createBucketContents(2),
            List.of(new Prefix("prefix1/"), new Prefix("prefix2/")), "continuationToken", "2",
            "nextContinuationToken", "startAfter", "url");

    serializeAndAssert(iut, testInfo);
  }

  private List<S3Object> createBucketContents(int count) {
    List<S3Object> s3ObjectList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      S3Object s3Object =
          new S3Object("key" + i, "2009-10-12T17:50:30.000Z",
              "\"fba9dede5f27731c9771645a39863328\"", "434234", StorageClass.STANDARD,
              new Owner(String.valueOf(10L + i), "displayName"));
      s3ObjectList.add(s3Object);
    }
    return s3ObjectList;
  }
}
