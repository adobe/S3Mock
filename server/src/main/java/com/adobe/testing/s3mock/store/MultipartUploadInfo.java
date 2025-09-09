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

package com.adobe.testing.s3mock.store;

import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Encapsulates {@link MultipartUpload} and corresponding {@code contentType}.
 */
public record MultipartUploadInfo(
    MultipartUpload upload,
    @Nullable String contentType,
    Map<String, String> userMetadata,
    Map<String, String> storeHeaders,
    Map<String, String> encryptionHeaders,
    String bucket,
    @Nullable StorageClass storageClass,
    @Nullable List<Tag> tags,
    @Nullable String checksum,
    @Nullable ChecksumType checksumType,
    @Nullable ChecksumAlgorithm checksumAlgorithm,
    boolean completed
) {

  public MultipartUploadInfo(
      MultipartUpload upload,
      String contentType,
      Map<String, String> userMetadata,
      Map<String, String> storeHeaders,
      Map<String, String> encryptionHeaders,
      String bucket,
      StorageClass storageClass,
      List<Tag> tags,
      String checksum,
      ChecksumType checksumType,
      ChecksumAlgorithm checksumAlgorithm
  ) {
    this(
        upload,
        contentType,
        userMetadata,
        storeHeaders,
        encryptionHeaders,
        bucket,
        storageClass,
        tags,
        checksum,
        checksumType,
        checksumAlgorithm,
        false
    );
  }

  public MultipartUploadInfo complete() {
    return new MultipartUploadInfo(
        this.upload,
        this.contentType,
        this.userMetadata,
        this.storeHeaders,
        this.encryptionHeaders,
        this.bucket,
        this.storageClass,
        this.tags,
        this.checksum,
        this.checksumType,
        this.checksumAlgorithm,
        true
    );
  }
}
