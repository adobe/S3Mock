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

package com.adobe.testing.s3mock.store

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

abstract class StoreBase {
  /**
   * Stores the content of an InputStream in a File.
   * Creates the File if it does not exist.
   * Uses buffered streams with a fixed buffer size to optimize memory usage for large files.
   *
   * @param inputPath the incoming binary data to be saved.
   * @param filePath Path where the stream should be saved.
   *
   * @return the newly created File.
   */
  fun inputPathToFile(inputPath: Path, filePath: Path): File {
    val targetFile = filePath.toFile()
    try {
      targetFile.createNewFile()
      BufferedInputStream(Files.newInputStream(inputPath), BUFFER_SIZE).use { input ->
        BufferedOutputStream(Files.newOutputStream(targetFile.toPath()), BUFFER_SIZE).use { os ->
          input.transferTo(os)
        }
      }
    } catch (e: IOException) {
      throw IllegalStateException("Could not write object binary-file.", e)
    }
    return targetFile
  }

  companion object {
    private val BUFFER_SIZE = 1024 * 1024
  }
}
