/*
 *  Copyright 2017-2022 Adobe.
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

import com.adobe.testing.s3mock.dto.Tag;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents an object in S3, used to serialize and deserialize all metadata locally.
 */
public class S3ObjectMetadata {

  private UUID id;

  private String name;

  private String size;

  private String modificationDate;

  private String md5;

  private String etag;

  private String contentType;

  private String contentEncoding;

  private String kmsEncryption;

  private boolean isEncrypted;

  private long lastModified;

  private Path dataPath;

  private String kmsKeyId;

  private Map<String, String> userMetadata;

  private List<Tag> tags;

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getSize() {
    return size;
  }

  public void setSize(final String size) {
    this.size = size;
  }

  public String getModificationDate() {
    return modificationDate;
  }

  public void setModificationDate(final String modificationDate) {
    this.modificationDate = modificationDate;
  }

  public String getEtag() {
    return etag;
  }

  public void setEtag(String etag) {
    this.etag = etag;
  }

  public String getMd5() {
    return md5;
  }

  public void setMd5(final String md5) {
    this.md5 = md5;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(final String contentType) {
    this.contentType = contentType;
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public void setContentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

  public Path getDataPath() {
    return dataPath;
  }

  public void setDataPath(final Path dataPath) {
    this.dataPath = dataPath;
  }

  public String getKmsEncryption() {
    return kmsEncryption;
  }

  public void setKmsEncryption(final String kmsEncryption) {
    this.kmsEncryption = kmsEncryption;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public void setEncrypted(final boolean isEncrypted) {
    this.isEncrypted = isEncrypted;
  }

  public long getLastModified() {
    return lastModified;
  }

  public void setLastModified(final long lastModified) {
    this.lastModified = lastModified;
  }

  public String getKmsKeyId() {
    return kmsKeyId;
  }

  public void setKmsKeyId(final String kmsKeyId) {
    this.kmsKeyId = kmsKeyId;
  }

  public Map<String, String> getUserMetadata() {
    return userMetadata == null ? Collections.emptyMap() : userMetadata;
  }

  public void setUserMetadata(final Map<String, String> userMetadata) {
    this.userMetadata = userMetadata;
  }

  public void setTags(final List<Tag> tag) {
    this.tags = tag;
  }

  public List<Tag> getTags() {
    return tags == null ? new ArrayList<>() : tags;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }
}
