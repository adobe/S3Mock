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

import com.adobe.testing.s3mock.domain.BucketContents;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ListBucketResultTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    ListBucketResult iut =
        new ListBucketResult("bucketName", "prefix/", "marker", 1000, false, "url", "nextMarker",
            createBucketContents(2), Arrays.asList("prefix1/", "prefix2/"));

    serializeAndAssert(iut, testInfo);
  }

  private List<BucketContents> createBucketContents(int count) {
    List<BucketContents> bucketContentsList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      BucketContents bucketContents =
          new BucketContents("key" + i, "2009-10-12T17:50:30.000Z",
              "fba9dede5f27731c9771645a39863328", "434234", "STANDARD",
              new Owner(10L + i, "displayName"));
      bucketContentsList.add(bucketContents);
    }
    return bucketContentsList;
  }
}
