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

import static java.util.Collections.emptyMap;

import com.adobe.testing.s3mock.dto.Owner;
import java.io.File;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import org.apache.http.entity.ContentType;
import org.springframework.beans.factory.annotation.Autowired;

abstract class StoreTestBase {
  static final String TEST_BUCKET_NAME = "test-bucket";
  static final String TEST_FILE_PATH = "src/test/resources/sampleFile.txt";
  static final String NO_ENC = null;
  static final String NO_ENC_KEY = null;
  static final Map<String, String> NO_USER_METADATA = emptyMap();
  static final String TEST_ENC_TYPE = "aws:kms";
  static final String TEST_ENC_KEY = "aws:kms" + UUID.randomUUID();
  static final String TEXT_PLAIN = ContentType.TEXT_PLAIN.toString();
  static final String ENCODING_GZIP = "gzip";
  static final String NO_PREFIX = null;
  static final String DEFAULT_CONTENT_TYPE =
      ContentType.APPLICATION_OCTET_STREAM.toString();
  static final Owner TEST_OWNER = new Owner("123", "s3-mock-file-store");

  @Autowired
  private File rootFolder;

  BucketMetadata metadataFrom(String bucketName) {
    BucketMetadata metadata = new BucketMetadata();
    metadata.setName(bucketName);
    metadata.setPath(Paths.get(rootFolder.toString(), bucketName));
    return metadata;
  }

}
