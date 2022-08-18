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

package com.adobe.testing.s3mock.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletedObject.html">API Reference</a>.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DeletedS3Object extends S3ObjectIdentifier {

  @JsonProperty("DeleteMarker")
  private Boolean deleteMarker;

  @JsonProperty("DeleteMarkerVersionId")
  private String deleteMarkerVersionId;

  public Boolean getDeleteMarker() {
    return deleteMarker;
  }

  public void setDeleteMarker(Boolean deleteMarker) {
    this.deleteMarker = deleteMarker;
  }

  public String getDeleteMarkerVersionId() {
    return deleteMarkerVersionId;
  }

  public void setDeleteMarkerVersionId(String deleteMarkerVersionId) {
    this.deleteMarkerVersionId = deleteMarkerVersionId;
  }

  public static DeletedS3Object from(S3ObjectIdentifier s3ObjectIdentifier) {
    DeletedS3Object deletedObject = new DeletedS3Object();
    deletedObject.setKey(s3ObjectIdentifier.getKey());
    deletedObject.setVersionId(s3ObjectIdentifier.getVersionId());
    return deletedObject;
  }
}
