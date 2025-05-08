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

import static org.apache.commons.io.FileUtils.cleanDirectory;

import java.io.File;
import org.springframework.beans.factory.DisposableBean;

public class StoreCleaner implements DisposableBean {

  private final File rootFolder;
  private final boolean retainFilesOnExit;

  public StoreCleaner(File rootFolder, boolean retainFilesOnExit) {
    this.rootFolder = rootFolder;
    this.retainFilesOnExit = retainFilesOnExit;
  }

  @Override
  public void destroy() throws Exception {
    if (!retainFilesOnExit && rootFolder.exists()) {
      cleanDirectory(rootFolder);
    }
  }
}
