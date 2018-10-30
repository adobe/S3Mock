/*
 *  Copyright 2017-2018 Adobe.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.ArrayList;
import java.util.List;

/**
 * Result to be returned after batch delete request.
 */
@JsonRootName("DeleteResult")
public class BatchDeleteResponse {

  @JsonProperty("Deleted")
  @JacksonXmlElementWrapper(useWrapping = false)
  private final List<DeletedObject> deletedObjects = new ArrayList<>();

  public List<DeletedObject> getDeletedObjects() {
    return deletedObjects;
  }

  public void addDeletedObject(final DeletedObject deletedObject) {
    deletedObjects.add(deletedObject);
  }
}
