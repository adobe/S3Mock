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

package com.adobe.testing.s3mock.service;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.store.BucketMetadata;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.ObjectStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.test.mock.mockito.MockBean;

abstract class ServiceTestBase {

  static final String[] ALL_OBJECTS =
      new String[] {"3330/0", "33309/0", "a",
          "b", "b/1", "b/1/1", "b/1/2", "b/2",
          "c/1", "c/1/1",
          "d:1", "d:1:1",
          "eor.txt", "foo/eor.txt"};

  @MockBean
  BucketStore bucketStore;
  @MockBean
  ObjectStore objectStore;

  BucketMetadata givenBucket(String name) {
    when(bucketStore.doesBucketExist(eq(name))).thenReturn(true);
    BucketMetadata bucketMetadata = metadataFrom(name);
    when(bucketStore.getBucketMetadata(eq(name))).thenReturn(bucketMetadata);
    return bucketMetadata;
  }

  List<S3Object> givenBucketWithContents(String name, String prefix) {
    BucketMetadata bucketMetadata = givenBucket(name);
    List<S3Object> s3Objects = givenBucketContents(prefix);
    List<UUID> ids = new ArrayList<>();
    for (S3Object s3Object : s3Objects) {
      UUID id = bucketMetadata.addKey(s3Object.key());
      ids.add(id);
      when(objectStore.getS3ObjectMetadata(bucketMetadata, id))
          .thenReturn(s3ObjectMetadata(id, s3Object.key()));
    }
    when(bucketStore.lookupKeysInBucket(prefix, name)).thenReturn(ids);
    return s3Objects;
  }

  List<S3Object> givenBucketWithContents(String name, String prefix, List<S3Object> s3Objects) {
    BucketMetadata bucketMetadata = givenBucket(name);
    List<UUID> ids = new ArrayList<>();
    for (S3Object s3Object : s3Objects) {
      UUID id = bucketMetadata.addKey(s3Object.key());
      ids.add(id);
      when(objectStore.getS3ObjectMetadata(bucketMetadata, id))
          .thenReturn(s3ObjectMetadata(id, s3Object.key()));
    }
    when(bucketStore.lookupKeysInBucket(prefix, name)).thenReturn(ids);
    return s3Objects;
  }

  List<S3Object> givenBucketContents() {
    return givenBucketContents(null);
  }

  List<S3Object> givenBucketContents(String prefix) {
    List<S3Object> list = new ArrayList<>();
    for (String object : ALL_OBJECTS) {
      if (StringUtils.isNotEmpty(prefix)) {
        if (!object.startsWith(prefix)) {
          continue;
        }
      }
      list.add(givenS3Object(object));
    }
    return list;
  }

  S3Object givenS3Object(String key) {
    String lastModified = "lastModified";
    String etag = "etag";
    String size = "size";
    Owner owner = new Owner(String.valueOf(0L), "name");
    return new S3Object(key, lastModified, etag, size, StorageClass.STANDARD, owner);
  }

  S3ObjectMetadata s3ObjectMetadata(UUID id, String key) {
    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata();
    s3ObjectMetadata.setId(id);
    s3ObjectMetadata.setKey(key);
    s3ObjectMetadata.setModificationDate("1234");
    s3ObjectMetadata.setEtag("\"someetag\"");
    s3ObjectMetadata.setSize("size");
    return s3ObjectMetadata;
  }

  BucketMetadata metadataFrom(String bucketName) {
    BucketMetadata metadata = new BucketMetadata();
    metadata.setName(bucketName);
    metadata.setPath(Paths.get(FileUtils.getTempDirectoryPath(), bucketName));
    return metadata;
  }

  List<Part> givenParts(int count, long size) {
    List<Part> parts = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Date lastModified = new Date();
      parts.add(new Part(i, "\"" + UUID.randomUUID() + "\"", lastModified, size));
    }
    return parts;
  }
}
