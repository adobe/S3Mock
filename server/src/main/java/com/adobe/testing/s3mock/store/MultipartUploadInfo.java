/*
 *  Copyright 2017-2024 Adobe.
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

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.StorageClass;
import java.util.Map;

/**
 * Encapsulates {@link MultipartUpload} and corresponding {@code contentType}.
 */
public record MultipartUploadInfo(MultipartUpload upload,
                           String contentType,
                           Map<String, String> userMetadata,
                           Map<String, String> storeHeaders,
                           Map<String, String> encryptionHeaders,
                           String bucket,
                           StorageClass storageClass,
                           String checksum,
                           ChecksumAlgorithm checksumAlgorithm) {

}
