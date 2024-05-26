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

package com.adobe.testing.s3mock;

import com.adobe.testing.s3mock.util.AwsHttpHeaders;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;

/**
 * Converts values of the {@link AwsHttpHeaders#X_AMZ_OBJECT_OWNERSHIP} which is sent by the Amazon
 * client.
 * Example:  x-amz-object-ownership: ObjectWriter
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html">API Reference</a>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_OwnershipControlsRule.html">API Reference</a>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html">API Reference</a>
 */
class ObjectOwnershipHeaderConverter implements Converter<String, ObjectOwnership> {

  @Override
  @Nullable
  public ObjectOwnership convert(@NonNull String source) {
    return ObjectOwnership.fromValue(source);
  }
}
