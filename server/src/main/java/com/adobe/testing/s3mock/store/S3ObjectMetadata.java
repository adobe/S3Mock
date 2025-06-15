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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.springframework.http.MediaType;

/**
 * Represents an object in S3, used to serialize and deserialize all metadata locally.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html">See API</a>
 */
public record S3ObjectMetadata(
    UUID id,
    String key,
    String size,
    String modificationDate,
    @Nullable String etag,
    @Nullable String contentType,
    long lastModified,
    Path dataPath,
    @Nullable Map<String, String> userMetadata,
    @Nullable List<Tag> tags,
    @Nullable LegalHold legalHold,
    @Nullable Retention retention,
    Owner owner,
    @Nullable Map<String, String> storeHeaders,
    @Nullable Map<String, String> encryptionHeaders,
    @Nullable ChecksumAlgorithm checksumAlgorithm,
    @Nullable String checksum,
    @Nullable StorageClass storageClass,
    @Nullable AccessControlPolicy policy,
    @Nullable String versionId,
    boolean deleteMarker,
    @Nullable ChecksumType checksumType
) {

  public S3ObjectMetadata {
    etag = normalizeEtag(etag);
    contentType = Objects.requireNonNullElse(contentType, MediaType.APPLICATION_OCTET_STREAM_VALUE);
    userMetadata = userMetadata == null ? Collections.emptyMap() : userMetadata;
    tags = Objects.requireNonNullElse(tags, new ArrayList<>());
    storeHeaders = storeHeaders == null ? Collections.emptyMap() : storeHeaders;
    encryptionHeaders = encryptionHeaders == null ? Collections.emptyMap() : encryptionHeaders;
    storageClass = storageClass == StorageClass.STANDARD ? null : storageClass;
    checksumType = checksumType == null ? ChecksumType.FULL_OBJECT : checksumType;
  }

  public static S3ObjectMetadata deleteMarker(S3ObjectMetadata metadata, String versionId) {
    return new S3ObjectMetadata(metadata.id,
        metadata.key(),
        metadata.size(),
        metadata.modificationDate(),
        metadata.etag(),
        metadata.contentType(),
        metadata.lastModified(),
        metadata.dataPath(),
        metadata.userMetadata(),
        metadata.tags(),
        metadata.legalHold(),
        metadata.retention(),
        metadata.owner(),
        metadata.storeHeaders(),
        metadata.encryptionHeaders(),
        metadata.checksumAlgorithm(),
        metadata.checksum(),
        metadata.storageClass(),
        metadata.policy(),
        versionId,
        true,
        metadata.checksumType()
    );
  }
}
