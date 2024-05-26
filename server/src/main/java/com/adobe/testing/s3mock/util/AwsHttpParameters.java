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

package com.adobe.testing.s3mock.util;

/**
 * Holds Parameters used in HTTP requests from AWS S3 Client.
 */
public class AwsHttpParameters {

  private static final String NOT = "!";

  public static final String ACL = "acl";
  public static final String NOT_ACL = NOT + ACL;
  public static final String CONTINUATION_TOKEN = "continuation-token";
  public static final String DELETE = "delete";
  public static final String ENCODING_TYPE = "encoding-type";
  public static final String KEY_MARKER = "key-marker";
  public static final String VERSION_ID_MARKER = "version-id-marker";
  public static final String LIST_TYPE_V2 = "list-type=2";
  public static final String VERSIONS = "versions";
  public static final String NOT_VERSIONS = "!versions";
  public static final String NOT_LIST_TYPE = "!list-type";
  public static final String MAX_KEYS = "max-keys";
  public static final String PART_NUMBER = "partNumber";
  public static final String START_AFTER = "start-after";
  public static final String TAGGING = "tagging";
  public static final String NOT_TAGGING = NOT + TAGGING;
  public static final String UPLOADS = "uploads";
  public static final String NOT_UPLOADS = NOT + UPLOADS;

  public static final String UPLOAD_ID = "uploadId";
  public static final String NOT_UPLOAD_ID = NOT + UPLOAD_ID;

  public static final String LEGAL_HOLD = "legal-hold";
  public static final String NOT_LEGAL_HOLD = NOT + LEGAL_HOLD;
  public static final String OBJECT_LOCK = "object-lock";
  public static final String NOT_OBJECT_LOCK = NOT + OBJECT_LOCK;
  public static final String RETENTION = "retention";
  public static final String NOT_RETENTION = NOT + RETENTION;
  public static final String LIFECYCLE = "lifecycle";
  public static final String NOT_LIFECYCLE = NOT + LIFECYCLE;
  public static final String ATTRIBUTES = "attributes";
  public static final String NOT_ATTRIBUTES = NOT + ATTRIBUTES;
  public static final String LOCATION = "location";
  public static final String NOT_LOCATION = NOT + LOCATION;

  public static final String VERSION_ID = "versionId";

  private AwsHttpParameters() {
    // private constructor for utility classes
  }
}
