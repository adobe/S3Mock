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

import com.adobe.testing.S3Verified;
import org.jspecify.annotations.Nullable;

/**
 * <a href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">API Reference</a>.
 */
@S3Verified(year = 2025)
public enum Region {
  AP_SOUTH_2("ap-south-2"),
  AP_SOUTH_1("ap-south-1"),
  EU_SOUTH_1("eu-south-1"),
  EU_SOUTH_2("eu-south-2"),
  US_GOV_EAST_1("us-gov-east-1"),
  ME_CENTRAL_1("me-central-1"),
  IL_CENTRAL_1("il-central-1"),
  US_ISOF_SOUTH_1("us-isof-south-1"),
  CA_CENTRAL_1("ca-central-1"),
  MX_CENTRAL_1("mx-central-1"),
  EU_CENTRAL_1("eu-central-1"),
  US_ISO_WEST_1("us-iso-west-1"),
  EUSC_DE_EAST_1("eusc-de-east-1"),
  EU_CENTRAL_2("eu-central-2"),
  EU_ISOE_WEST_1("eu-isoe-west-1"),
  US_WEST_1("us-west-1"),
  US_WEST_2("us-west-2"),
  AF_SOUTH_1("af-south-1"),
  EU_NORTH_1("eu-north-1"),
  EU_WEST_3("eu-west-3"),
  EU_WEST_2("eu-west-2"),
  EU_WEST_1("eu-west-1"),
  AP_NORTHEAST_3("ap-northeast-3"),
  AP_NORTHEAST_2("ap-northeast-2"),
  AP_NORTHEAST_1("ap-northeast-1"),
  ME_SOUTH_1("me-south-1"),
  SA_EAST_1("sa-east-1"),
  AP_EAST_1("ap-east-1"),
  CN_NORTH_1("cn-north-1"),
  CA_WEST_1("ca-west-1"),
  US_GOV_WEST_1("us-gov-west-1"),
  AP_SOUTHEAST_1("ap-southeast-1"),
  AP_SOUTHEAST_2("ap-southeast-2"),
  US_ISO_EAST_1("us-iso-east-1"),
  AP_SOUTHEAST_3("ap-southeast-3"),
  AP_SOUTHEAST_4("ap-southeast-4"),
  AP_SOUTHEAST_5("ap-southeast-5"),
  US_EAST_1("us-east-1"),
  US_EAST_2("us-east-2"),
  AP_SOUTHEAST_7("ap-southeast-7"),
  CN_NORTHWEST_1("cn-northwest-1"),
  US_ISOB_EAST_1("us-isob-east-1"),
  US_ISOF_EAST_1("us-isof-east-1");

  private final String value;

  Region(String value) {
    this.value = value;
  }

  @Nullable
  public static Region fromValue(String value) {
    for (Region region : Region.values()) {
      if (region.value.equals(value)) {
        return region;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return this.value;
  }
}
