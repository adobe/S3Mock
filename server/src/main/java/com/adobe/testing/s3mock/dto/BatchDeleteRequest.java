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
import java.util.List;

/**
 * Request to initiate a batch delete request.
 */
@JsonRootName("Delete")
public class BatchDeleteRequest {

  @JsonProperty("Quiet")
  private boolean quiet;

  @JsonProperty("Object")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<ObjectToDelete> objectsToDelete;

  public List<ObjectToDelete> getObjectsToDelete() {
    return objectsToDelete;
  }

  public void setObjectsToDelete(final List<ObjectToDelete> objectsToDelete) {
    this.objectsToDelete = objectsToDelete;
  }

  public boolean isQuiet() {
    return quiet;
  }

  public void setQuiet(final boolean quiet) {
    this.quiet = quiet;
  }

  public static class ObjectToDelete {

    @JsonProperty("Key")
    private String key;

    @JsonProperty("VersionId")
    private String versionId;

    public String getKey() {
      return key;
    }

    public void setKey(final String key) {
      this.key = key;
    }

    public String getVersionId() {
      return versionId;
    }

    public void setVersionId(final String versionId) {
      this.versionId = versionId;
    }
  }
}
