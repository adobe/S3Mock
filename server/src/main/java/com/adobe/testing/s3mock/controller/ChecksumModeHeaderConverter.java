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

package com.adobe.testing.s3mock.controller;

import com.adobe.testing.s3mock.dto.ChecksumMode;
import com.adobe.testing.s3mock.util.AwsHttpHeaders;
import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;

/**
 * Converts values of the {@link AwsHttpHeaders#X_AMZ_CHECKSUM_MODE} which is sent by the Amazon
 * client.
 * Example:  x-amz-checksum-mode: ENABLED
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>
 */
class ChecksumModeHeaderConverter implements Converter<String, ChecksumMode> {

  @Override
  @Nullable
  public ChecksumMode convert(String source) {
    return ChecksumMode.fromValue(source);
  }
}
