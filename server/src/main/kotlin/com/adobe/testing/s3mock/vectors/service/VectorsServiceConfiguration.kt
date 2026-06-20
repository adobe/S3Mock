/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.vectors.service

import com.adobe.testing.s3mock.vectors.store.VectorBucketStore
import com.adobe.testing.s3mock.vectors.store.VectorIndexStore
import com.adobe.testing.s3mock.vectors.store.VectorStore
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import software.amazon.awssdk.regions.Region

@Configuration
@Profile("vectors")
class VectorsServiceConfiguration {
  @Bean
  fun vectorBucketService(
    vectorBucketStore: VectorBucketStore,
    @Value($$"${com.adobe.testing.s3mock.store.region}") region: Region?,
  ): VectorBucketService = VectorBucketService(vectorBucketStore, (region ?: Region.US_EAST_1).id())

  @Bean
  fun vectorIndexService(
    vectorBucketService: VectorBucketService,
    vectorIndexStore: VectorIndexStore,
    @Value($$"${com.adobe.testing.s3mock.store.region}") region: Region?,
  ): VectorIndexService = VectorIndexService(vectorBucketService, vectorIndexStore, (region ?: Region.US_EAST_1).id())

  @Bean
  fun vectorService(
    vectorIndexService: VectorIndexService,
    vectorStore: VectorStore,
  ): VectorService = VectorService(vectorIndexService, vectorStore)

  @Bean
  fun vectorQueryService(
    vectorIndexService: VectorIndexService,
    vectorStore: VectorStore,
  ): VectorQueryService = VectorQueryService(vectorIndexService, vectorStore)
}
