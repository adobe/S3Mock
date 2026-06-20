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
package com.adobe.testing.s3mock.vectors.store

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import tools.jackson.databind.ObjectMapper
import java.io.File

@Configuration
@Profile("vectors")
class VectorsStoreConfiguration {
  @Bean
  fun vectorsRoot(rootFolder: File): File {
    val dir = rootFolder.resolve("vectors")
    dir.mkdirs()
    return dir
  }

  @Bean
  fun vectorBucketStore(
    vectorsRoot: File,
    objectMapper: ObjectMapper,
  ): VectorBucketStore = VectorBucketStore(vectorsRoot, objectMapper)

  @Bean
  fun vectorIndexStore(
    vectorBucketStore: VectorBucketStore,
    objectMapper: ObjectMapper,
  ): VectorIndexStore = VectorIndexStore(vectorBucketStore, objectMapper)

  @Bean
  fun vectorStore(
    vectorIndexStore: VectorIndexStore,
    objectMapper: ObjectMapper,
  ): VectorStore = VectorStore(vectorIndexStore, objectMapper)
}
