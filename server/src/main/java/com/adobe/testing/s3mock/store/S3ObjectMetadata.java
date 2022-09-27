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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents an object in S3, used to serialize and deserialize all metadata locally.
 */
public record S3ObjectMetadata(
    UUID id,
    String key,
    String size,
    String modificationDate,
    String etag,
    String contentType,
    long lastModified,
    Path dataPath,
    Map<String, String> userMetadata,
    List<Tag> tags,
    LegalHold legalHold,
    Retention retention,
    Owner owner,
    Map<String, String> storeHeaders,
    Map<String, String> encryptionHeaders
) {

  private static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";

  public S3ObjectMetadata(UUID id, String key, String size, String modificationDate,
      String etag, String contentType, long lastModified, Path dataPath,
      Map<String, String> userMetadata, List<Tag> tags, LegalHold legalHold, Retention retention,
      Owner owner, Map<String, String> storeHeaders, Map<String, String> encryptionHeaders) {
    this.id = id;
    this.key = key;
    this.size = size;
    this.modificationDate = modificationDate;
    this.etag = normalizeEtag(etag);
    this.contentType = Objects.requireNonNullElse(contentType, DEFAULT_CONTENT_TYPE);
    this.lastModified = lastModified;
    this.dataPath = dataPath;
    this.userMetadata = userMetadata == null ? Collections.emptyMap() : userMetadata;
    this.tags = Objects.requireNonNullElse(tags, new ArrayList<>());
    this.legalHold = legalHold;
    this.retention = retention;
    this.owner = owner;
    this.storeHeaders = storeHeaders == null ? Collections.emptyMap() : storeHeaders;
    this.encryptionHeaders = encryptionHeaders == null ? Collections.emptyMap() : encryptionHeaders;
  }

}
