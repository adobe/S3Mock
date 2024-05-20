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

package com.adobe.testing.s3mock.store;

import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

abstract class StoreBase {

  /**
   * Stores the content of an InputStream in a File.
   * Creates the File if it does not exist.
   *
   * @param inputPath the incoming binary data to be saved.
   * @param filePath Path where the stream should be saved.
   *
   * @return the newly created File.
   */
  File inputPathToFile(Path inputPath, Path filePath, boolean retainFilesOnExit) {
    var targetFile = filePath.toFile();
    try {
      if (targetFile.createNewFile() && (!retainFilesOnExit)) {
        targetFile.deleteOnExit();
      }

      try (var is = newInputStream(inputPath);
           var os = newOutputStream(targetFile.toPath())) {
        is.transferTo(os);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not write object binary-file.", e);
    }
    return targetFile;
  }
}
