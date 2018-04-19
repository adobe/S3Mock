/*
 *  Copyright 2017-2018 Adobe.
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

import org.testng.IExecutionListener;

/**
 * TestNG listener to start and stop the S3Mock Application. After the tests, the S3Mock is
 * stopped.
 *
 * <h3>Configuring through testng.xml file</h3>
 * <pre>
 * {@code
 * <?xml version="1.0" encoding="UTF-8"?>
 * <!DOCTYPE suite SYSTEM "http://testng.org/testng-1.0.dtd">
 * <suite name="TestNG Listener Example">
 *  <listeners>
 *      <listener class-name="com.adobe.testing.s3mock.testng.S3ExecutionListener" />
 *  </listeners>
 *
 *  <test name="TestNG Sample Test" preserve-order="true">
 *      <classes>
 *          <class name="SampleS3MockTest">
 *              <methods>
 *                  <include name="test1"/>
 *              </methods>
 *          </class>
 *      </classes>
 *  </test>
 * </suite>
 * }
 * </pre>
 */

public class S3MockListener implements IExecutionListener {
    @Override
    public void onExecutionStart() {
        S3Mock.getInstance().bootstrap();
    }

    @Override
    public void onExecutionFinish() {
        S3Mock.getInstance().terminate();
    }
}
