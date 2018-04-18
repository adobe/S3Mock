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

import com.adobe.testing.s3mock.S3MockApplication;
import com.adobe.testing.s3mock.testsupport.common.S3MockStarter;

import java.util.HashMap;
import java.util.Map;

public class S3Mock extends S3MockStarter {
    private static S3Mock instance = null;

    private S3Mock() {
        super(null);
        this.properties.putAll(defaultProps());
        final Map<String, Object> properties = new HashMap<>();

        String httpsPort =  System.getProperty(S3MockApplication.PROP_HTTPS_PORT);
        if(httpsPort != null) {
            properties.put(S3MockApplication.PROP_HTTPS_PORT,httpsPort);
        } else {
            properties.put(S3MockApplication.PROP_HTTPS_PORT,S3MockApplication.DEFAULT_HTTPS_PORT);
        }

        String httpPort = System.getProperty(S3MockApplication.PROP_HTTP_PORT);
        if(httpPort != null) {
            properties.put(S3MockApplication.PROP_HTTP_PORT,httpPort);
        } else {
            properties.put(S3MockApplication.PROP_HTTP_PORT,S3MockApplication.DEFAULT_HTTP_PORT);
        }

        String initialBuckets = System.getProperty(S3MockApplication.PROP_INITIAL_BUCKETS);
        if(initialBuckets != null) {
            properties.put(S3MockApplication.PROP_INITIAL_BUCKETS,initialBuckets);
        }

        String rootDirectory = System.getProperty(S3MockApplication.PROP_ROOT_DIRECTORY);
        if(rootDirectory != null) {
            properties.put(S3MockApplication.PROP_ROOT_DIRECTORY,rootDirectory);
        }

        this.properties.putAll(properties);
    }

    public static S3Mock getInstance() {
        if(instance == null) instance = new S3Mock();
        return instance;
    }

    void bootstrap() {
        this.start();
    }

    void terminate() {
        this.stop();
    }
}
