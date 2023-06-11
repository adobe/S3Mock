/*
 *  Copyright 2017-2023 Adobe.
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

package com.adobe.testing.s3mock.store;

import java.util.List;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("com.adobe.testing.s3mock.domain") //TODO: wrong package.
public record StoreProperties(
    // True if files should be retained when S3Mock exits gracefully.
    // False to let S3Mock delete all files when S3Mock exits gracefully.
    boolean retainFilesOnExit,
    // The root directory to use. If omitted a default temp-dir will be used.
    String root,
    @DefaultValue
    Set<String> validKmsKeys,
    // A comma separated list of buckets that are to be created at startup.
    @DefaultValue
    List<String> initialBuckets
) {

}
