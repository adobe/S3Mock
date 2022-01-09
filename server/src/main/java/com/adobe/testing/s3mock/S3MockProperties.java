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

package com.adobe.testing.s3mock;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("com.adobe.testing.s3mock")
class S3MockProperties {

  /**
   * Property name for passing the HTTPS port to use. Defaults to
   * {@value S3MockApplication#DEFAULT_HTTPS_PORT}. If set to
   * {@value S3MockApplication#RANDOM_PORT}, a random port will be chosen.
   */
  private int httpPort;

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }
}
