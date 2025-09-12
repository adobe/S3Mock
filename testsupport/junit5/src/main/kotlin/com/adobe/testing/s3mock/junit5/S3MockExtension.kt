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
package com.adobe.testing.s3mock.junit5

import com.adobe.testing.s3mock.S3MockApplication
import com.adobe.testing.s3mock.testsupport.common.S3MockStarter
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolutionException
import org.junit.jupiter.api.extension.ParameterResolver
import software.amazon.awssdk.services.s3.S3Client

/**
 * JUnit extension to start and stop the S3Mock Application. After the tests, the S3Mock is
 * stopped.
 *
 *
 * The following modes are supported:
 *
 * <h2>1. Programmatic registration</h2>
 * <pre>
 * public class MyS3Test {
 * &#64;RegisterExtension
 * public static S3MockExtension S3_MOCK = S3MockExtension.builder().build();
 *
 * private final AmazonS3 s3Client = S3_MOCK.createS3Client();
 *
 * &#64;Test
 * public void doSomethingWithS3() {
 * s3Client.createBucket("myBucket");
 * }
 * }
</pre> *
 *
 * <h2>2. Declarative registration</h2>
 * <pre>
 * &#64;ExtendWith(S3MockExtension.class)
 * public class MyS3Test {
 *
 * &#64;Test
 * public void doSomethingWithS3(final AmazonS3 s3Client) {
 * s3Client.createBucket("myBucket");
 * }
 * }
</pre> *
 */
class S3MockExtension : S3MockStarter, BeforeAllCallback, AfterAllCallback, ParameterResolver {
  private var mockAccess = 0

  /**
   * Creates an instance with the default configuration.
   */
  constructor() : super(null)

  private constructor(properties: Map<String, Any>) : super(properties)

  override fun beforeAll(context: ExtensionContext) {
    startOnlySingleInstance()
  }

  override fun afterAll(context: ExtensionContext) {
    stopWhenLastConsumerFinished()
  }

  @Throws(ParameterResolutionException::class)
  override fun supportsParameter(
    parameterContext: ParameterContext,
    extensionContext: ExtensionContext
  ): Boolean {
    return paramHasType(parameterContext, S3MockApplication::class.java)
      || paramHasType(parameterContext, S3Client::class.java)
  }

  @Throws(ParameterResolutionException::class)
  override fun resolveParameter(
    parameterContext: ParameterContext,
    extensionContext: ExtensionContext
  ): Any? {
    if (paramHasType(parameterContext, S3MockApplication::class.java)) {
      return s3MockFileStore
    }

    if (paramHasType(parameterContext, S3Client::class.java)) {
      return createS3ClientV2()
    }

    return null
  }

  private fun paramHasType(parameterContext: ParameterContext, cls: Class<*>): Boolean {
    val requiredType = parameterContext.getParameter().getType()
    return requiredType.isAssignableFrom(cls)
  }

  @Synchronized
  private fun stopWhenLastConsumerFinished() {
    if (--mockAccess == 0) {
      stop()
    }
  }

  @Synchronized
  private fun startOnlySingleInstance() {
    if (mockAccess++ == 0) {
      start()
    }
  }

  /**
   * Builder for S3MockExtension.
   */
  class Builder : BaseBuilder<S3MockExtension>() {
    override fun build(): S3MockExtension {
      return S3MockExtension(arguments)
    }
  }

  companion object {
    /**
     * Builder instance.
     *
     * @return builder instance.
     */
    @JvmStatic
    fun builder(): Builder {
      return Builder()
    }
  }
}
