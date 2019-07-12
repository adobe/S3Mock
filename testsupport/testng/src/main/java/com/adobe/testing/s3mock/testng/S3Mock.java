/*
 *  Copyright 2017-2019 Adobe.
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

package com.adobe.testing.s3mock.testng;

import com.adobe.testing.s3mock.testsupport.common.S3MockStarter;

/**
 * Singleton extending {@link com.adobe.testing.s3mock.testsupport.common.S3MockStarter}.
 * <p>Used in the {@link com.adobe.testing.s3mock.testng.S3MockListener} to start
 * {@link com.adobe.testing.s3mock.S3MockApplication} when TestNG starts running the suites and to
 * stop when TestNG has run all the suites</p>
 */
public class S3Mock extends S3MockStarter {

  private static final S3Mock INSTANCE = new S3Mock();

  private S3Mock() {
    super(null);
  }

  public static S3Mock getInstance() {
    return INSTANCE;
  }

  void bootstrap() {
    start();
  }

  void terminate() {
    stop();
  }
}
