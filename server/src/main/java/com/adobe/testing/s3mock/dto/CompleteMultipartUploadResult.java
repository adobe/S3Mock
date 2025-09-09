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

import com.adobe.testing.s3mock.store.MultipartUploadInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

/**
 * Result to be returned when completing a multipart request.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">API Reference</a>
 */
@JsonRootName("CompleteMultipartUploadResult")
public record CompleteMultipartUploadResult(
    @JsonProperty("Bucket") String bucket,
    @JsonProperty("ChecksumCRC32") String checksumCRC32,
    @JsonProperty("ChecksumCRC32C") String checksumCRC32C,
    @JsonProperty("ChecksumCRC64NVME") String checksumCRC64NVME,
    @JsonProperty("ChecksumSHA1") String checksumSHA1,
    @JsonProperty("ChecksumSHA256") String checksumSHA256,
    @JsonProperty("ChecksumType") ChecksumType checksumType,
    @JsonProperty("ETag") String etag,
    @JsonProperty("Key") String key,
    @JsonProperty("Location") String location,
    // workaround for adding xmlns attribute to root element only.
    @JacksonXmlProperty(isAttribute = true, localName = "xmlns") String xmlns,
    @JsonIgnore MultipartUploadInfo multipartUploadInfo,
    @JsonIgnore String versionId,
    @JsonIgnore String checksum
) {
  public CompleteMultipartUploadResult {
    etag = normalizeEtag(etag);
    if (xmlns == null) {
      xmlns = "http://s3.amazonaws.com/doc/2006-03-01/";
    }
  }

  public static CompleteMultipartUploadResult from(String location,
       String bucket,
       String key,
       String etag,
       MultipartUploadInfo multipartUploadInfo,
       String checksum,
       ChecksumType checksumType,
       ChecksumAlgorithm checksumAlgorithm,
       String versionId) {

    var usedAlgorithm = checksumAlgorithm != null ? checksumAlgorithm : multipartUploadInfo.checksumAlgorithm();
    var usedChecksum = checksum != null ? checksum : multipartUploadInfo.checksum();

    if (usedChecksum == null) {
      return new CompleteMultipartUploadResult(bucket, null, null, null, null, null,
          checksumType, etag, key, location, null, multipartUploadInfo, versionId, null);
    }

    return switch (usedAlgorithm) {
      case CRC32 ->
          new CompleteMultipartUploadResult(bucket,
              usedChecksum, null, null, null, null,
              checksumType, etag, key, location, null, multipartUploadInfo, versionId, checksum);
      case CRC32C ->
          new CompleteMultipartUploadResult(bucket,
              null, usedChecksum, null, null, null,
              checksumType, etag, key, location, null, multipartUploadInfo, versionId, checksum);
      case CRC64NVME ->
          new CompleteMultipartUploadResult(bucket,
              null, null, usedChecksum, null, null,
              checksumType, etag, key, location, null, multipartUploadInfo, versionId, checksum);
      case SHA1 ->
          new CompleteMultipartUploadResult(bucket,
              null, null, null, usedChecksum, null,
              checksumType, etag, key, location, null, multipartUploadInfo, versionId, checksum);
      case SHA256 ->
          new CompleteMultipartUploadResult(bucket,
              null, null, null, null, usedChecksum,
              checksumType, etag, key, location, null, multipartUploadInfo, versionId, checksum);
    };
  }
}
