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

import com.adobe.testing.s3mock.dto.Region;
import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;

/**
 * Converts values of the {@link com.adobe.testing.s3mock.util.AwsHttpParameters#BUCKET_REGION} which
 * is sent by the Amazon client.
 * Example:  bucket-region: us-east-1
 * <a href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">API Reference</a>
 */
class RegionConverter implements Converter<String, Region> {

  @Override
  @Nullable
  public Region convert(String source) {
    return Region.fromValue(source);
  }
}
