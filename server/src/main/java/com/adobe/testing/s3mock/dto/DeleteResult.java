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

package com.adobe.testing.s3mock.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;

/**
 * Result to be returned after batch delete request.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">API Reference</a>
 */
@JsonRootName("DeleteResult")
public record DeleteResult(
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("Deleted") List<DeletedS3Object> deletedObjects,
    @JacksonXmlElementWrapper(useWrapping = false)
    @JsonProperty("Error") List<Error> errors,
    //workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns
) {

  public DeleteResult {
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public DeleteResult(List<Error> errors, List<DeletedS3Object> deletedObjects) {
    this(deletedObjects, errors, null);
  }

  public void addDeletedObject(DeletedS3Object deletedObject) {
    deletedObjects.add(deletedObject);
  }

  public void addError(Error error) {
    errors.add(error);
  }
}
