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

package com.adobe.testing.s3mock.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

public class StoreCleaner implements CommandLineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(StoreCleaner.class);
  private final File rootFolder;
  private final boolean retainFilesOnExit;

  public StoreCleaner(File rootFolder, boolean retainFilesOnExit) {
    this.rootFolder = rootFolder;
    this.retainFilesOnExit = retainFilesOnExit;
  }

  @Override
  public void run(String... args) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        LOG.info("Calling StoreCleaner destroy() with retainFilesOnExit={}", retainFilesOnExit);
        if (!retainFilesOnExit && rootFolder.exists()) {
          Files.walk(rootFolder.toPath())
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
          LOG.info("Directory {} cleaned up via shutdown hook.", rootFolder);
        }
      } catch (IOException e) {
        LOG.error("Error cleaning up directory {}", rootFolder, e);
      }
    }));
  }
}
