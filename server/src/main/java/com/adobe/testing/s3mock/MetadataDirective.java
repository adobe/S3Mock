/*
 *  Copyright 2017-2021 Adobe.
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

/**
 * This enum declares values of the optional "x-amz-metadata-directive" header.
 * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html</p>
 */
public enum MetadataDirective {

  COPY,
  REPLACE;

  static final String METADATA_DIRECTIVE_COPY = "COPY";
  static final String METADATA_DIRECTIVE_REPLACE = "REPLACE";

}
