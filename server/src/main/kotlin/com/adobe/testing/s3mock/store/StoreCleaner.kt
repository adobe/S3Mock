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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import java.io.File

open class StoreCleaner(
  private val rootFolder: File,
  private val retainFilesOnExit: Boolean,
) : DisposableBean {
  override fun destroy() {
    LOG.info("Calling StoreCleaner destroy() with retainFilesOnExit={}", retainFilesOnExit)
    if (!retainFilesOnExit && rootFolder.exists()) {
      try {
        rootFolder.listFiles()?.forEach { it.deleteRecursively() }
        LOG.info("Directory {} cleaned up.", rootFolder)
      } catch (e: Exception) {
        LOG.warn("Could not clean up directory {}.", rootFolder, e)
      }
    }
  }

  companion object {
    val LOG: Logger = LoggerFactory.getLogger(StoreCleaner::class.java)
  }
}
