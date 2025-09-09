/*
 *  Copyright 2017-2025 Adobe.
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

package com.adobe.testing.s3mock;

import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.util.AwsHttpHeaders;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;

/**
 * Converts values of the {@link AwsHttpHeaders#X_AMZ_TAGGING} which is sent by the Amazon client.
 * Example: x-amz-tagging: tag1=value1&tag2=value2
 *
 * <p></p>
 * It also converts XML tags into a list of {@link Tag} objects.
 * Example: '<?xml version=\"1.0\" encoding=\"UTF-8\"?><Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag></TagSet></Tagging>'
 *
 * <p></p>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html">API Reference</a>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">API Reference</a>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html">API Reference</a>
 */
class TaggingHeaderConverter implements Converter<String, List<Tag>> {
  private static final String XML_START = "<";
  private static final String XML_END = ">";

  private final XmlMapper xmlMapper;

  public TaggingHeaderConverter(XmlMapper xmlMapper) {
    this.xmlMapper = xmlMapper;
  }

  @Override
  @Nullable
  public List<Tag> convert(String source) {
    if (source.startsWith(XML_START) && source.endsWith(XML_END)) {
      return convertTagXml(source);
    }

    return convertTagPairs(source);
  }

  @Nullable
  private List<Tag> convertTagXml(String source) {
    try {
      var tagging = this.xmlMapper.readValue(source, Tagging.class);
      if (tagging.tagSet() != null) {
        return tagging.tagSet().tags();
      }
      return null;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse XML tags from header: " + source, e);
    }
  }

  @Nullable
  private static List<Tag> convertTagPairs(String source) {
    var tags = new ArrayList<Tag>();
    String[] tagPairs = StringUtils.split(source, '&');
    for (String tag : tagPairs) {
      tags.add(new Tag(tag));
    }
    return tags.isEmpty() ? null : tags;
  }
}
