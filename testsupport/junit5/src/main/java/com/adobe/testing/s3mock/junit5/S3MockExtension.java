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

package com.adobe.testing.s3mock.junit5;

import com.adobe.testing.s3mock.S3MockApplication;
import com.adobe.testing.s3mock.testsupport.common.S3MockStarter;
import com.amazonaws.services.s3.AmazonS3;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * JUnit extension to start and stop the S3Mock Application. After the tests, the S3Mock is
 * stopped.
 *
 * <p>The following modes are supported:</p>
 *
 * <h2>1. Programmatic registration</h2>
 * <pre>
 * public class MyS3Test {
 *   &#64;RegisterExtension
 *   public static S3MockExtension S3_MOCK = S3MockExtension.builder().build();
 *
 *   private final AmazonS3 s3Client = S3_MOCK.createS3Client();
 *
 *   &#64;Test
 *   public void doSomethingWithS3() {
 *     s3Client.createBucket("myBucket");
 *   }
 * }
 * </pre>
 *
 * <h2>2. Declarative registration</h2>
 * <pre>
 * &#64;ExtendWith(S3MockExtension.class)
 * public class MyS3Test {
 *
 *   &#64;Test
 *   public void doSomethingWithS3(final AmazonS3 s3Client) {
 *     s3Client.createBucket("myBucket");
 *   }
 * }
 * </pre>
 */
public class S3MockExtension extends S3MockStarter implements BeforeAllCallback, AfterAllCallback,
    ParameterResolver {

  private int mockAccess;

  /**
   * Builder instance.
   *
   * @return builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates an instance with the default configuration.
   */
  public S3MockExtension() {
    super(null);
  }

  private S3MockExtension(final Map<String, Object> properties) {
    super(properties);
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    startOnlySingleInstance();
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    stopWhenLastConsumerFinished();
  }

  @Override
  public boolean supportsParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    return paramHasType(parameterContext, S3MockApplication.class)
        || paramHasType(parameterContext, AmazonS3.class)
        || paramHasType(parameterContext, S3Client.class);
  }

  @Nullable
  @Override
  public Object resolveParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {

    if (paramHasType(parameterContext, S3MockApplication.class)) {
      return s3MockFileStore;
    }

    if (paramHasType(parameterContext, AmazonS3.class)) {
      return createS3Client();
    }

    if (paramHasType(parameterContext, S3Client.class)) {
      return createS3ClientV2();
    }

    return null;
  }

  private boolean paramHasType(final ParameterContext parameterContext, final Class<?> cls) {
    var requiredType = parameterContext.getParameter().getType();
    return requiredType.isAssignableFrom(cls);
  }

  private synchronized void stopWhenLastConsumerFinished() {
    if (--mockAccess == 0) {
      stop();
    }
  }

  private synchronized void startOnlySingleInstance() {
    if (mockAccess++ == 0) {
      start();
    }
  }

  /**
   * Builder for S3MockExtension.
   */
  public static class Builder extends S3MockStarter.BaseBuilder<S3MockExtension> {

    @Override
    public S3MockExtension build() {
      return new S3MockExtension(arguments);
    }
  }
}
