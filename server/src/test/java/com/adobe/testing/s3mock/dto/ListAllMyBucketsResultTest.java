/*
 *  Copyright 2017-2023 Adobe.
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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ListAllMyBucketsResultTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    var iut =
        new ListAllMyBucketsResult(
            new Owner(String.valueOf(10L), "displayName"), createBuckets(2)
        );
    assertThat(iut).isNotNull();
    serializeAndAssert(iut, testInfo);
  }

  private Buckets createBuckets(int count) {
    var buckets = new ArrayList<Bucket>();
    for (var i = 0; i < count; i++) {
      var bucket = new Bucket(Paths.get("/tmp/foo"), "name", "creationDate");
      buckets.add(bucket);
    }
    return new Buckets(buckets);
  }

}
