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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ListAllMyBucketsResultTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    ListAllMyBucketsResult iut =
        new ListAllMyBucketsResult(
            new Owner(String.valueOf(10L), "displayName"), createBuckets(2)
        );

    serializeAndAssert(iut, testInfo);
  }

  private Buckets createBuckets(int count) {
    List<Bucket> buckets = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Bucket bucket = new Bucket(Paths.get("/tmp/foo"), "name", "creationDate");
      buckets.add(bucket);
    }
    return new Buckets(buckets);
  }

}
