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

package com.adobe.testing.s3mock.dto;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * A DTO which can be used as an response body if an error occurred.
 *
 * @see <a href="http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">AWS REST
 * Error Response</a>
 */
@XStreamAlias("Error")
public class ErrorResponse {

  @XStreamAlias("Code")
  private String code;

  @XStreamAlias("Message")
  private String message;

  @XStreamAlias("Resource")
  private String resource;

  @XStreamAlias("RequestId")
  private String requestId;

  /**
   * @param code The Error code
   * @see <a href=
   *      "http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList">
   *      AWS Error Codes</a>
   */
  public void setCode(final String code) {
    this.code = code;
  }

  /**
   * @param message A description of the error.
   */
  public void setMessage(final String message) {
    this.message = message;
  }

  /**
   * @param resource The bucket or object that is involved in the error.
   */
  public void setResource(final String resource) {
    this.resource = resource;
  }

  /**
   * @param requestId ID of the request associated with the error.
   */
  public void setRequestId(final String requestId) {
    this.requestId = requestId;
  }

  /**
   * Returns a xml representation of this Error.
   *
   * @return xml String
   */
  public String toXML() {
    final XStream xStream = new XStream();
    xStream.processAnnotations(ErrorResponse.class);
    return xStream.toXML(this);
  }

}
