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
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletedObject.html
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DeletedObject extends ObjectIdentifier {

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

  public static DeletedObject from(ObjectIdentifier objectIdentifier) {
    DeletedObject deletedObject = new DeletedObject();
    deletedObject.setKey(objectIdentifier.getKey());
    deletedObject.setVersionId(objectIdentifier.getVersionId());
    return deletedObject;
  }
}
