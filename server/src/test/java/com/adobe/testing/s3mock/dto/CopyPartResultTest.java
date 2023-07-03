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

import java.io.IOException;
import java.util.Date;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class CopyPartResultTest {

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    var iut = CopyPartResult.from(new Date(1514477008120L), "99f2fdceebf20fb2e891810adfb0eb71");

    serializeAndAssert(iut, testInfo);
  }

}
