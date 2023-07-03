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

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Result to be returned for GetObjectTagging.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html">API Reference</a>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Tagging.html">API Reference</a>
 */
@JsonRootName("Tagging")
public record Tagging(
    @JacksonXmlProperty(localName = "TagSet")
    TagSet tagSet
) {

}
