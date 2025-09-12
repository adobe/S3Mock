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

import org.apache.commons.io.FileUtils
import org.springframework.beans.factory.DisposableBean
import java.io.File

open class StoreCleaner(private val rootFolder: File, private val retainFilesOnExit: Boolean) : DisposableBean {
    @Throws(Exception::class)
    override fun destroy() {
        if (!retainFilesOnExit && rootFolder.exists()) {
            FileUtils.cleanDirectory(rootFolder)
        }
    }
}
