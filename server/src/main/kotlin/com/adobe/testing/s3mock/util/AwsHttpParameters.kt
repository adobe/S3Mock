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

package com.adobe.testing.s3mock.util

/**
 * Holds Parameters used in HTTP requests from AWS S3 Client.
 */
object AwsHttpParameters {
  const val BUCKET_REGION: String = "bucket-region"
  const val CONTINUATION_TOKEN: String = "continuation-token"
  const val ENCODING_TYPE: String = "encoding-type"
  const val FETCH_OWNER: String = "fetch-owner"
  const val FILE: String = "file"
  const val KEY: String = "key"
  const val KEY_MARKER: String = "key-marker"
  const val VERSION_ID_MARKER: String = "version-id-marker"
  const val LIST_TYPE_V2: String = "list-type=2"
  const val NOT_LIST_TYPE: String = "!list-type"
  const val MAX_BUCKETS: String = "max-buckets"
  const val MAX_KEYS: String = "max-keys"
  const val MAX_PARTS: String = "max-parts"
  const val MAX_UPLOADS: String = "max-uploads"
  const val PART_NUMBER: String = "partNumber"
  const val PART_NUMBER_MARKER: String = "part-number-marker"
  const val PREFIX: String = "prefix"
  const val START_AFTER: String = "start-after"
  const val UPLOAD_ID_MARKER: String = "upload-id-marker"
  const val VERSION_ID: String = "versionId"

  private const val NOT = "!"

  const val DELETE: String = "delete"
  const val NOT_DELETE: String = NOT + DELETE
  const val ACL: String = "acl"
  const val NOT_ACL: String = NOT + ACL
  const val VERSIONS: String = "versions"
  const val NOT_VERSIONS: String = NOT + VERSIONS
  const val TAGGING: String = "tagging"
  const val NOT_TAGGING: String = NOT + TAGGING
  const val UPLOADS: String = "uploads"
  const val NOT_UPLOADS: String = NOT + UPLOADS
  const val UPLOAD_ID: String = "uploadId"
  const val NOT_UPLOAD_ID: String = NOT + UPLOAD_ID
  const val LEGAL_HOLD: String = "legal-hold"
  const val NOT_LEGAL_HOLD: String = NOT + LEGAL_HOLD
  const val OBJECT_LOCK: String = "object-lock"
  const val NOT_OBJECT_LOCK: String = NOT + OBJECT_LOCK
  const val RETENTION: String = "retention"
  const val NOT_RETENTION: String = NOT + RETENTION
  const val LIFECYCLE: String = "lifecycle"
  const val NOT_LIFECYCLE: String = NOT + LIFECYCLE
  const val ATTRIBUTES: String = "attributes"
  const val NOT_ATTRIBUTES: String = NOT + ATTRIBUTES
  const val LOCATION: String = "location"
  const val NOT_LOCATION: String = NOT + LOCATION
  const val VERSIONING: String = "versioning"
  const val NOT_VERSIONING: String = NOT + VERSIONING
}
