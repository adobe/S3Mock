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

package com.adobe.testing.s3mock.junit4;

import com.adobe.testing.s3mock.testsupport.common.S3MockStarter;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule to start and stop the S3Mock Application. After the tests, the S3Mock is stopped. It
 * should be used as {@link ClassRule}:
 *
 * <pre>
 * &#64;ClassRule
 * public static S3MockRule S3_MOCK_RULE = S3MockRule.builder().build();
 *
 * private final AmazonS3 s3Client = S3_MOCK_RULE.createS3Client();
 *
 * &#64;Test
 * public void doSomethingWithS3() {
 *   s3Client.createBucket("myBucket");
 * }
 * </pre>
 *
 * @deprecated JUnit 5 was released over two years ago. Please migrate your JUnit 4 tests to JUnit 5
 *    This TestRule will be removed in S3Mock 4.x
 */
@Deprecated(since = "3.0.0", forRemoval = true)
public class S3MockRule extends S3MockStarter implements TestRule {

  /**
   * Creates an instance with the default configuration.
   */
  public S3MockRule() {
    super(null);
  }

  public static Builder builder() {
    return new Builder();
  }

  private S3MockRule(final Map<String, Object> properties) {
    super(properties);
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        try {
          base.evaluate();
        } finally {
          stop();
        }
      }
    };
  }

  public static class Builder extends S3MockStarter.BaseBuilder<S3MockRule> {

    @Override
    public S3MockRule build() {
      return new S3MockRule(arguments);
    }
  }
}
