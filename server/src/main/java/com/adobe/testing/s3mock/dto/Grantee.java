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

package com.adobe.testing.s3mock.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.OptBoolean;
import java.net.URI;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Grantee.html">API Reference</a>.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "xsi:type",
    requireTypeIdForSubtypes = OptBoolean.TRUE)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = CanonicalUser.class, name = "CanonicalUser"),
    @JsonSubTypes.Type(value = Group.class, name = "Group"),
    @JsonSubTypes.Type(value = AmazonCustomerByEmail.class, name = "AmazonCustomerByEmail")
})
public interface Grantee {

  URI AUTHENTICATED_USERS_URI = URI.create("http://acs.amazonaws.com/groups/global/AuthenticatedUsers");
  URI ALL_USERS_URI = URI.create("http://acs.amazonaws.com/groups/global/AllUsers");
  URI LOG_DELIVERY_URI = URI.create("http://acs.amazonaws.com/groups/s3/LogDelivery");

  String id();

  String displayName();

  String emailAddress();

  URI uri();

}
