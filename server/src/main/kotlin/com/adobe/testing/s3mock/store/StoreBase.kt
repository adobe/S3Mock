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

package com.adobe.testing.s3mock.store

import tools.jackson.databind.ObjectMapper
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

abstract class StoreBase {
  /**
   * One lock object per UUID — shared across [ObjectStore] and [MultipartStore] subclasses.
   * Guards read-modify-write access to metadata files that can be updated concurrently for the
   * same id (e.g. object or multipart-upload metadata); not required for per-operation files
   * that are never contended, such as individual part binaries.
   */
  private val lockStore: MutableMap<UUID, Any> = ConcurrentHashMap()

  protected fun lockFor(id: UUID): Any = lockStore.computeIfAbsent(id) { Any() }

  /**
   * Serialise [value] to [file] under the per-[lockId] lock, converting [IOException] to
   * [IllegalStateException] with [context] as the error message prefix.
   */
  protected fun <T : Any> writeLockedJson(
    lockId: UUID,
    file: File,
    value: T,
    context: String,
    objectMapper: ObjectMapper,
  ) {
    try {
      synchronized(lockFor(lockId)) { objectMapper.writeValue(file, value) }
    } catch (e: IOException) {
      throw IllegalStateException("Could not write $context", e)
    }
  }

  fun inputPathToFile(
    inputPath: Path,
    filePath: Path,
  ): File {
    try {
      Files.copy(inputPath, filePath, REPLACE_EXISTING)
    } catch (e: IOException) {
      throw IllegalStateException("Could not write object binary-file.", e)
    }
    return filePath.toFile()
  }
}
