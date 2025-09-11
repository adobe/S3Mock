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

package com.adobe.testing.s3mock;

import org.apache.catalina.connector.Connector;
import org.apache.tomcat.util.buf.EncodedSolidusHandling;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.servlet.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(S3MockProperties.class)
public class S3MockConfiguration {
  private Connector httpConnector;

  /**
   * Create a ServletWebServerFactory bean reconfigured for an additional HTTP port.
   *
   * @return webServerFactory bean reconfigured for an additional HTTP port
   */
  @Bean
  ServletWebServerFactory webServerFactory(S3MockProperties properties) {
    var factory = new TomcatServletWebServerFactory();
    factory.addConnectorCustomizers(connector -> {
      // Allow encoded slashes in URL
      connector.setEncodedSolidusHandling(EncodedSolidusHandling.DECODE.getValue());
      connector.setAllowBackslash(true);
    });
    factory.addAdditionalConnectors(createHttpConnector(properties.httpPort()));
    return factory;
  }

  private Connector createHttpConnector(int httpPort) {
    httpConnector = new Connector();
    httpConnector.setPort(httpPort);
    return httpConnector;
  }

  Connector getHttpConnector() {
    return httpConnector;
  }

}
