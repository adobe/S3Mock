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

import java.io.File;

/**
 * Holds S3 object metadata.
 */
public class S3Object {
  private String name;

  private String size;

  private String creationDate;

  private String modificationDate;

  private String md5;

  private String contentType;

  private String kmsEncryption;

  private boolean isEncrypted;

  private long lastModified;

  private transient File dataFile = null;

  private String kmsKeyId;

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

  public String getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(final String creationDate) {
    this.creationDate = creationDate;
  }

  public String getModificationDate() {
    return modificationDate;
  }

  public void setModificationDate(final String modificationDate) {
    this.modificationDate = modificationDate;
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

  public File getDataFile() {
    return dataFile;
  }

  public void setDataFile(final File dataFile) {
    this.dataFile = dataFile;
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

  public void setKmsEncryptionKeyId(final String kmsKeyId) {
    this.kmsKeyId = kmsKeyId;
  }
}
