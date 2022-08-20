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

package com.adobe.testing.s3mock.testsupport.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.S3MockApplication;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;

public class S3MockStarterTest {

  /**
   * Tests startup and shutdown of S3MockApplication.
   */
  @Test
  void testS3MockApplication() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(S3MockApplication.PROP_HTTPS_PORT, S3MockApplication.RANDOM_PORT);
    properties.put(S3MockApplication.PROP_HTTP_PORT, S3MockApplication.RANDOM_PORT);
    properties.put(S3MockApplication.PROP_INITIAL_BUCKETS, "bucket");

    S3MockStarterTestImpl s3MockApplication = new S3MockStarterTestImpl(properties);
    s3MockApplication.start();

    assertThat(s3MockApplication.getHttpPort()).isPositive();
    List<Bucket> buckets;
    try (S3Client s3ClientV2 = s3MockApplication.createS3ClientV2()) {
      buckets = s3ClientV2.listBuckets().buckets();
    }
    assertThat(buckets.get(0).name()).isEqualTo("bucket");

    s3MockApplication.stop();
  }

  /**
   * Just needed to instantiate the S3MockStarter.
   * The instance provides an S3Client that is pre-configured to connect to the S3MockApplication.
   */
  private static class S3MockStarterTestImpl extends S3MockStarter {
    protected S3MockStarterTestImpl(Map<String, Object> properties) {
      super(properties);
    }
  }
}
