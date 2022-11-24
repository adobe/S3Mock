/*
 *  Copyright 2017-2022 Adobe.
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

package com.adobe.testing.s3mock.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.commons.io.IOUtils.copyLarge;

public class IOUtil {
  
  private static final long MAX_HEAP_SIZE = Runtime.getRuntime().maxMemory();
  private static final int COPY_LARGE_THRESHOLD_FACTOR = 5;

  public static void heapAwareCopy(Path path, OutputStream outputStream) throws IOException {
    long fileSize = path.toFile().length();
    if (fileSize * COPY_LARGE_THRESHOLD_FACTOR > MAX_HEAP_SIZE) {
      try (InputStream is = new FileInputStream(path.toFile())) {
        copyLarge(is, outputStream);
      }
    } else {
      Files.copy(path, outputStream);
    }
  }


}
