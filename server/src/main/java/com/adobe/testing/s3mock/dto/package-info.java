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

/**
 * This set the namespace for all JAX-B serialization / deserialization POJOs in this package.
 */
@XmlSchema(namespace = "http://s3.amazonaws.com/doc/2006-03-01/",
    xmlns = { @XmlNs(prefix = "", namespaceURI = "http://s3.amazonaws.com/doc/2006-03-01/") },
    elementFormDefault = XmlNsForm.QUALIFIED)
package com.adobe.testing.s3mock.dto;

import jakarta.xml.bind.annotation.XmlNs;
import jakarta.xml.bind.annotation.XmlNsForm;
import jakarta.xml.bind.annotation.XmlSchema;
