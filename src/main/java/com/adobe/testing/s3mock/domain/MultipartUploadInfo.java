/*
 *  Copyright 2017 Adobe.
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

package com.adobe.testing.s3mock.domain;

import com.adobe.testing.s3mock.dto.MultipartUpload;

import java.util.Map;

/**
 * Encapsulates {@link MultipartUpload} and corresponding {@code contentType}
 */
class MultipartUploadInfo {
  final MultipartUpload upload;
  final String contentType;
  final Map<String, String> userMetadata;

  MultipartUploadInfo(final MultipartUpload upload, final String contentType, Map<String, String> userMetadata) {
    this.upload = upload;
    this.contentType = contentType;
    this.userMetadata = userMetadata;
  }
}
