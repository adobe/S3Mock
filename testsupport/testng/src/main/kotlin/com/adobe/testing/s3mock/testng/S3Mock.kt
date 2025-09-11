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
package com.adobe.testing.s3mock.testng

import com.adobe.testing.s3mock.testsupport.common.S3MockStarter

/**
 * Singleton extending [S3MockStarter].
 *
 *
 * Used in the [S3MockListener] to start
 * [com.adobe.testing.s3mock.S3MockApplication] when TestNG starts running the suites and to
 * stop when TestNG has run all the suites
 */
class S3Mock private constructor() : S3MockStarter(null) {
  fun bootstrap() {
    start()
  }

  fun terminate() {
    stop()
  }

  companion object {
    /**
     * Returns an instance of S3Mock.
     *
     * @return an instance of S3Mock
     */
    private val _instance: S3Mock = S3Mock()

    @JvmStatic
    fun getInstance(): S3Mock = _instance
  }
}
