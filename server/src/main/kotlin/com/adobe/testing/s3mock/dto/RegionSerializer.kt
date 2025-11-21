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
package com.adobe.testing.s3mock.dto

import tools.jackson.core.JsonGenerator
import tools.jackson.databind.SerializationContext
import tools.jackson.databind.ValueSerializer
import java.io.IOException

/**
 * Serialize AWS Region objects.
 * [API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html#API_GetBucketLocation_ResponseSyntax)
 */
class RegionSerializer : ValueSerializer<Region>() {
  @Throws(IOException::class)
  override fun serialize(value: Region, gen: JsonGenerator, serializers: SerializationContext?) {
    val regionString: String = value.toString()
    // API doc says to return "null" for the us-east-1 region.
    if ("us-east-1" == regionString) {
      gen.writeString("null")
    } else {
      gen.writeString(regionString)
    }
  }
}
