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
import org.springframework.boot.CommandLineRunner
import java.io.File
import java.io.IOException

open class StoreCleaner(
  private val rootFolder: File,
  private val retainFilesOnExit: Boolean,
) : CommandLineRunner {
  @Throws(Exception::class)
  override fun run(vararg args: String) {
    Runtime.getRuntime().addShutdownHook(
      Thread({
        try {
          LOG.info("Calling StoreCleaner destroy() with retainFilesOnExit={}", retainFilesOnExit)
          if (!retainFilesOnExit && rootFolder.exists()) {
            rootFolder.listFiles()?.forEach { it.deleteRecursively() }
            LOG.info("Directory {} cleaned up via shutdown hook.", rootFolder)
          }
        } catch (e: IOException) {
          LOG.error("Error cleaning up directory {}", rootFolder, e)
        }
      }),
    )
  }

  companion object {
    val LOG: Logger = LoggerFactory.getLogger(StoreCleaner::class.java)
  }
}
