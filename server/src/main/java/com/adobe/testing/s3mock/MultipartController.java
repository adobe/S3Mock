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

package com.adobe.testing.s3mock;

import static com.adobe.testing.s3mock.S3Exception.BAD_REQUEST_CONTENT;
import static com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_ALGORITHM;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_VERSION_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.KEY_MARKER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_PARTS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER_MARKER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID_MARKER;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromHeader;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumHeaderFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.encryptionHeadersFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.storeHeadersFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.userMetadataFrom;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.S3Verified;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumType;
import com.adobe.testing.s3mock.dto.CompleteMultipartUpload;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.CopySource;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.ObjectKey;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

@CrossOrigin(origins = "*", exposedHeaders = "*")
@Controller
@RequestMapping("${com.adobe.testing.s3mock.contextPath:}")
public class MultipartController {

  private final BucketService bucketService;
  private final ObjectService objectService;
  private final MultipartService multipartService;

  public MultipartController(BucketService bucketService, ObjectService objectService,
      MultipartService multipartService) {
    this.bucketService = bucketService;
    this.objectService = objectService;
    this.multipartService = multipartService;
  }

  //================================================================================================
  // /{bucketName:.+}
  //================================================================================================

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">API Reference</a>.
   */
  @GetMapping(
      value = {
          //AWS SDK V2 pattern
          "/{bucketName:.+}",
          //AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          UPLOADS
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<ListMultipartUploadsResult> listMultipartUploads(
      @PathVariable String bucketName,
      @RequestParam(required = false) String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) String encodingType,
      @RequestParam(name = KEY_MARKER, required = false) String keyMarker,
      @RequestParam(name = MAX_UPLOADS, defaultValue = "1000", required = false) Integer maxUploads,
      @RequestParam(required = false) String prefix,
      @RequestParam(name = UPLOAD_ID_MARKER, required = false) String uploadIdMarker) {
    bucketService.verifyBucketExists(bucketName);

    return ResponseEntity.ok(multipartService.listMultipartUploads(
            bucketName,
            delimiter,
            encodingType,
            keyMarker,
            maxUploads,
            prefix,
            uploadIdMarker
        )
    );
  }

  //================================================================================================
  // /{bucketName:.+}/{*key}
  //================================================================================================

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">API Reference</a>.
   */
  @DeleteMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID,
          NOT_LIFECYCLE
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> abortMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(bucketName, uploadId);
    multipartService.abortMultipartUpload(bucketName, key.key(), uploadId);
    return ResponseEntity.noContent().build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<ListPartsResult> listParts(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(name = MAX_PARTS, defaultValue = "1000", required = false) Integer maxParts,
      @RequestParam(name = PART_NUMBER_MARKER, required = false) Integer partNumberMarker,
      @RequestParam String uploadId) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(bucketName, uploadId);

    return ResponseEntity
        .ok(multipartService.getMultipartUploadParts(
            bucketName,
            key.key(),
            maxParts,
            partNumberMarker,
            uploadId)
        );
  }


  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">API Reference</a>.
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID,
          PART_NUMBER
      },
      headers = {
          NOT_X_AMZ_COPY_SOURCE,
          NOT_X_AMZ_COPY_SOURCE_RANGE
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> uploadPart(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId,
      @RequestParam String partNumber,
      @RequestHeader HttpHeaders httpHeaders,
      InputStream inputStream) {

    final var tempFileAndChecksum = multipartService.toTempFile(inputStream, httpHeaders);
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(bucketName, uploadId);
    multipartService.verifyPartNumberLimits(partNumber);

    String checksum = null;
    ChecksumAlgorithm checksumAlgorithm = null;
    var algorithmFromSdk = checksumAlgorithmFromSdk(httpHeaders);
    if (algorithmFromSdk != null) {
      checksum = tempFileAndChecksum.getRight();
      checksumAlgorithm = algorithmFromSdk;
    }
    var algorithmFromHeader = checksumAlgorithmFromHeader(httpHeaders);
    if (algorithmFromHeader != null) {
      checksum = checksumFrom(httpHeaders);
      checksumAlgorithm = algorithmFromHeader;
    }

    var tempFile = tempFileAndChecksum.getLeft();
    if (checksum != null) {
      multipartService.verifyChecksum(tempFile, checksum, checksumAlgorithm);
    }

    //persist checksum per part
    var etag = multipartService.putPart(bucketName,
        key.key(),
        uploadId,
        partNumber,
        tempFile,
        encryptionHeadersFrom(httpHeaders));

    FileUtils.deleteQuietly(tempFile.toFile());

    var checksumHeader = checksumHeaderFrom(checksum, checksumAlgorithm);
    return ResponseEntity
        .ok()
        .headers(h -> h.setAll(checksumHeader))
        .headers(h -> h.setAll(encryptionHeadersFrom(httpHeaders)))
        .eTag("\"" + etag + "\"")
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html">API Reference</a>.
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      headers = {
          X_AMZ_COPY_SOURCE,
      },
      params = {
          UPLOAD_ID,
          PART_NUMBER
      },
      produces = APPLICATION_XML_VALUE)
  @S3Verified(year = 2025)
  public ResponseEntity<CopyPartResult> uploadPartCopy(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_COPY_SOURCE) CopySource copySource,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_RANGE, required = false) HttpRange copyRange,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, required = false) List<Instant> ifModifiedSince,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE, required = false) List<Instant> ifUnmodifiedSince,
      @RequestParam String uploadId,
      @RequestParam String partNumber,
      @RequestHeader HttpHeaders httpHeaders) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    multipartService.verifyPartNumberLimits(partNumber);
    var s3ObjectMetadata = objectService.verifyObjectExists(copySource.bucket(), copySource.key(),
        copySource.versionId());
    objectService.verifyObjectMatchingForCopy(match, noneMatch,
        ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata);

    var encryptionHeaders = encryptionHeadersFrom(httpHeaders);
    var result = multipartService.copyPart(copySource.bucket(),
        copySource.key(),
        copyRange,
        partNumber,
        bucketName,
        key.key(),
        uploadId,
        encryptionHeaders,
        copySource.versionId()
    );

    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .headers(h -> {
          if (encryptionHeaders != null) {
            h.setAll(encryptionHeaders);
          }
        })
        .body(result);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html">API Reference</a>.
   */
  @PostMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOADS
      },
      produces = APPLICATION_XML_VALUE)
  @S3Verified(year = 2025)
  public ResponseEntity<InitiateMultipartUploadResult> createMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader(value = X_AMZ_CHECKSUM_TYPE, required = false) ChecksumType checksumType,
      @RequestHeader(value = X_AMZ_TAGGING, required = false) List<Tag> tags,
      @RequestHeader(value = X_AMZ_STORAGE_CLASS, required = false, defaultValue = "STANDARD")
      StorageClass storageClass,
      @RequestHeader HttpHeaders httpHeaders,
      InputStream inputStream) {
    bucketService.verifyBucketExists(bucketName);

    try {
      //workaround for AWS CRT-based S3 client: Consume (and discard) body in Initiate Multipart Upload request
      IOUtils.consume(inputStream);
    } catch (IOException e) {
      throw BAD_REQUEST_CONTENT;
    }

    var encryptionHeaders = encryptionHeadersFrom(httpHeaders);
    var checksumAlgorithm = checksumAlgorithmFromHeader(httpHeaders);
    var result =
        multipartService.createMultipartUpload(bucketName,
            key.key(),
            contentType,
            storeHeadersFrom(httpHeaders),
            DEFAULT_OWNER,
            DEFAULT_OWNER,
            userMetadataFrom(httpHeaders),
            encryptionHeaders,
            tags,
            storageClass,
            checksumType,
            checksumAlgorithm);

    return ResponseEntity
        .ok()
        .headers(h -> {
          if (encryptionHeaders != null) {
            h.setAll(encryptionHeaders);
          }
        })
        .headers(h -> {
          if (checksumAlgorithm != null) {
            h.set(X_AMZ_CHECKSUM_ALGORITHM, checksumAlgorithm.toString());
          }
        })
        .headers(h -> {
          if (checksumType != null) {
            h.set(X_AMZ_CHECKSUM_TYPE, checksumType.toString());
          }
        })
        .body(result);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">API Reference</a>.
   */
  @PostMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID
      },
      produces = APPLICATION_XML_VALUE)
  @S3Verified(year = 2025)
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestParam String uploadId,
      @RequestBody CompleteMultipartUpload upload,
      HttpServletRequest request,
      @RequestHeader HttpHeaders httpHeaders) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(bucketName, uploadId);
    multipartService.verifyMultipartParts(bucketName, key.key(), uploadId, upload.parts());
    var s3ObjectMetadata = objectService.getObject(bucketName, key.key(), null);
    objectService.verifyObjectMatching(match, noneMatch, null, null, s3ObjectMetadata);
    var objectName = key.key();
    var locationWithEncodedKey = request
        .getRequestURL()
        .toString()
        .replace(objectName, SdkHttpUtils.urlEncode(objectName));

    var result = multipartService.completeMultipartUpload(bucketName,
        key.key(),
        uploadId,
        upload.parts(),
        encryptionHeadersFrom(httpHeaders),
        locationWithEncodedKey,
        checksumFrom(httpHeaders),
        checksumAlgorithmFromHeader(httpHeaders)
    );

    return ResponseEntity
        .ok()
        .headers(h -> {
          if (result.multipartUploadInfo().encryptionHeaders() != null) {
            h.setAll(result.multipartUploadInfo().encryptionHeaders());
          }
        })
        .headers(h -> {
          if (bucket.isVersioningEnabled() && result.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, result.versionId());
          }
        })
        .body(result);
  }
}
