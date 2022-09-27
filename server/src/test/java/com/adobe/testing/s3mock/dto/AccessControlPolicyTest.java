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

import static com.adobe.testing.s3mock.dto.DtoTestUtil.getExpected;
import static com.adobe.testing.s3mock.dto.DtoTestUtil.getFile;
import static com.adobe.testing.s3mock.dto.Grant.Permission.FULL_CONTROL;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.util.XmlUtil;
import jakarta.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.xmlunit.assertj3.XmlAssert;

public class AccessControlPolicyTest {
  @Test
  void testDeserialization(TestInfo testInfo) throws IOException, XMLStreamException,
      JAXBException {
    File expected = getFile(testInfo);
    String contents = FileUtils.readFileToString(expected, StandardCharsets.UTF_8);
    AccessControlPolicy iut = XmlUtil.deserializeJaxb(contents);

    Owner owner = iut.getOwner();
    assertThat(owner).isNotNull();
    assertThat(owner.getId()).isEqualTo(
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a");
    assertThat(owner.getDisplayName()).isEqualTo("mtd@amazon.com");
    assertThat(iut.getAccessControlList()).hasSize(1);
    Grant grant = iut.getAccessControlList().get(0);
    assertThat(grant.getPermission()).isEqualTo(FULL_CONTROL);
    assertThat(grant.getGrantee()).isNotNull();
    assertThat(grant.getGrantee().getId()).isEqualTo(
        "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a");
    assertThat(grant.getGrantee().getDisplayName()).isEqualTo("mtd@amazon.com");
    assertThat(grant.getGrantee()).isInstanceOf(Grantee.CanonicalUser.class);
  }

  @Test
  void testSerialization(TestInfo testInfo) throws IOException, JAXBException {
    Owner owner = new Owner("75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a",
        "mtd@amazon.com");
    Grantee grantee = Grantee.from(owner);
    AccessControlPolicy iut = new AccessControlPolicy(owner,
        Collections.singletonList(new Grant(grantee, FULL_CONTROL))
    );
    String out = XmlUtil.serializeJaxb(iut);
    assertThat(out).isNotNull();
    String expected = getExpected(testInfo);
    XmlAssert.assertThat(out).and(expected)
        .ignoreChildNodesOrder()
        .ignoreWhitespace()
        .ignoreComments()
        .areIdentical();
  }
}
