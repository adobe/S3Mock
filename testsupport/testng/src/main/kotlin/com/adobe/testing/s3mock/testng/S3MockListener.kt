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
package com.adobe.testing.s3mock.testng

import org.testng.IExecutionListener

/**
 * TestNG listener to start and stop the S3Mock Application. After the tests, the S3Mock is
 * stopped.
 *
 * With Maven Surefire 3.6 or newer, configure the listener through the Surefire `listener` provider
 * property. Other TestNG runners may configure the listener through a `testng.xml` file:
 *
 * <pre>
 * `<?xml version="1.0" encoding="UTF-8"?>
 * <!DOCTYPE suite SYSTEM "http://testng.org/testng-1.0.dtd">
 * <suite name="TestNG Listener Example">
 * <listeners>
 * <listener class-name="com.adobe.testing.s3mock.testng.S3MockListener" />
 * </listeners>
 *
 * <test name="TestNG Sample Test" preserve-order="true">
 * <classes>
 * <class name="SampleS3MockTest">
 * <methods>
 * <include name="test1"/>
 * </methods>
 * </class>
 * </classes>
 * </test>
 * </suite>
 * `
 * </pre>
 *
 * When TestNG runs through the JUnit Platform, test classes may be instantiated during discovery.
 * Create clients in TestNG configuration methods or test methods instead of constructors or field
 * initializers.
 */
class S3MockListener : IExecutionListener {
  override fun onExecutionStart() = S3Mock.getInstance().bootstrap()

  override fun onExecutionFinish() = S3Mock.getInstance().terminate()
}
