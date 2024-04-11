/*
 *  Copyright 2017-2024 Adobe.
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
import static com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert;
import static com.adobe.testing.s3mock.dto.Grant.Permission.FULL_CONTROL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class AccessControlPolicyTest {
  @Test
  void testDeserialization(TestInfo testInfo) throws IOException {
    var iut = deserialize(AccessControlPolicy.class, testInfo);

    var owner = iut.getOwner();
    assertThat(owner).isNotNull();
    assertThat(owner.id()).isEqualTo(
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a");
    assertThat(owner.displayName()).isEqualTo("mtd@amazon.com");
    assertThat(iut.getAccessControlList()).hasSize(1);
    var grant = iut.getAccessControlList().get(0);
    assertThat(grant.permission()).isEqualTo(FULL_CONTROL);
    assertThat(grant.grantee()).isNotNull();
    assertThat(grant.grantee().id()).isEqualTo(
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a");
    assertThat(grant.grantee().displayName()).isEqualTo("mtd@amazon.com");
    assertThat(grant.grantee()).isInstanceOf(CanonicalUser.class);
  }

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    var owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    var grantee = new CanonicalUser(owner.id(), owner.displayName(), null, null);
    var iut = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );
    serializeAndAssert(iut, testInfo);
  }
}
