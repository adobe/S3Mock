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

package com.adobe.testing.s3mock.util;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Grantee;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Utility class with helper methods to serialize / deserialize JAXB annotated classes.
 */
public class XmlUtil {

  public static AccessControlPolicy deserializeJaxb(String toDeserialize)
      throws JAXBException, XMLStreamException {
    return deserializeJaxb(AccessControlPolicy.class, toDeserialize,
        AccessControlPolicy.class,
        Grantee.CanonicalUser.class,
        Grantee.Group.class,
        Grantee.AmazonCustomerByEmail.class);
  }

  public static <T> T deserializeJaxb(Class<T> clazz, String toDeserialize,
      Class<?>... additionalTypes)
      throws JAXBException, XMLStreamException {

    XMLStreamReader reader = XMLInputFactory.newInstance()
        .createXMLStreamReader(new StringReader(toDeserialize));
    JAXBContext jaxbContext = JAXBContext.newInstance(additionalTypes);
    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
    return jaxbUnmarshaller.unmarshal(reader, clazz).getValue();
  }

  public static String serializeJaxb(AccessControlPolicy toSerialize)
      throws JAXBException {
    return serializeJaxb(toSerialize, AccessControlPolicy.class,
        Grantee.CanonicalUser.class,
        Grantee.Group.class,
        Grantee.AmazonCustomerByEmail.class);
  }

  public static String serializeJaxb(Object toSerialize, Class<?>... additionalTypes)
      throws JAXBException {
    JAXBContext jaxbContext = JAXBContext.newInstance(additionalTypes);
    Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

    StringWriter writer = new StringWriter();
    jaxbMarshaller.marshal(toSerialize, writer);

    return writer.toString();
  }
}
