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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class ErrorResponseTest {
  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    ErrorResponse iut = new ErrorResponse();
    iut.setCode("code");
    iut.setMessage("message");
    iut.setRequestId("requestId");
    iut.setResource("resource");

    serializeAndAssert(iut, testInfo);
  }
}
