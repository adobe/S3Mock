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

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;

import com.adobe.testing.S3Verified;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.Date;

/**
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyPartResult.html">API Reference</a>.
 */
@S3Verified(year = 2025)
@JsonRootName("CopyPartResult")
public record CopyPartResult(
    @JsonProperty("ChecksumCRC32") String checksumCRC32,
    @JsonProperty("ChecksumCRC32C") String checksumCRC32C,
    @JsonProperty("ChecksumCRC64NVME") String checksumCRC64NVME,
    @JsonProperty("ChecksumSHA1") String checksumSHA1,
    @JsonProperty("ChecksumSHA256") String checksumSHA256,
    @JsonProperty("ETag") String etag,
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    @JsonProperty("LastModified") Date lastModified,
    // workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns
) {

  public CopyPartResult {
    etag = normalizeEtag(etag);
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public CopyPartResult(
      Date date, String etag
  ) {
    this(
        null,
        null,
        null,
        null,
        null,
        etag,
        date,
        null
    );
  }

  public CopyPartResult(
      ChecksumAlgorithm checksumAlgorithm,
      String checksum,
      String etag,
      Date lastModified) {
    this(
        checksumAlgorithm == ChecksumAlgorithm.CRC32 ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.CRC32C ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.CRC64NVME ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.SHA1 ? checksum : null,
        checksumAlgorithm == ChecksumAlgorithm.SHA256 ? checksum : null,
        etag,
        lastModified,
        null
    );
  }

  public static CopyPartResult from(final Date date, final String etag) {
    return new CopyPartResult(date, etag);
  }
}
