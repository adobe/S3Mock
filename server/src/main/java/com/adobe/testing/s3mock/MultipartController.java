/*
 *  Copyright 2017-2023 Adobe.
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

import static com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CONTENT_SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;
import static com.adobe.testing.s3mock.util.HeaderUtil.parseEncryptionHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.parseStoreHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.parseUserMetadata;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.CompleteMultipartUpload;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.CopySource;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.ObjectKey;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import jakarta.servlet.http.HttpServletRequest;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
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

/**
 * Handles requests related to parts.
 */
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
   * Lists all in-progress multipart uploads.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">API Reference</a>
   *
   * <p>Not yet supported request parameters: delimiter, encoding-type, max-uploads, key-marker,
   * upload-id-marker.</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link ListMultipartUploadsResult}
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
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListMultipartUploadsResult> listMultipartUploads(
      @PathVariable String bucketName,
      @RequestParam(required = false) String prefix) {
    bucketService.verifyBucketExists(bucketName);

    ListMultipartUploadsResult result =
        multipartService.listMultipartUploads(bucketName, prefix);

    return ResponseEntity.ok(result);
  }

  //================================================================================================
  // /{bucketName:.+}/{*key}
  //================================================================================================

  /**
   * Aborts a multipart upload for a given uploadId.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   */
  @DeleteMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID,
          NOT_LIFECYCLE
      },
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<Void> abortMultipartUpload(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(uploadId);
    multipartService.abortMultipartUpload(bucketName, key.key(), uploadId);
    return ResponseEntity.noContent().build();
  }

  /**
   * Lists all parts a file multipart upload.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   *
   * @return the {@link ListPartsResult}
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID
      },
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListPartsResult> listParts(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(uploadId);

    ListPartsResult result =
        multipartService.getMultipartUploadParts(bucketName, key.key(), uploadId);
    return ResponseEntity.ok(result);
  }


  /**
   * Adds an object to a bucket accepting encryption headers.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload.
   *
   * @return the etag of the uploaded part.
   *
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
  public ResponseEntity<Void> uploadPart(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId,
      @RequestParam String partNumber,
      @RequestHeader(value = X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      @RequestHeader HttpHeaders httpHeaders,
      InputStream inputStream) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(uploadId);
    multipartService.verifyPartNumberLimits(partNumber);

    String etag = multipartService.putPart(bucketName,
        key.key(),
        uploadId,
        partNumber,
        inputStream,
        isV4ChunkedWithSigningEnabled(sha256Header),
        parseEncryptionHeaders(httpHeaders));

    return ResponseEntity.ok().eTag("\"" + etag + "\"").build();
  }

  /**
   * Uploads a part by copying data from an existing object as data source.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html">API Reference</a>
   *
   * @param copySource References the Objects to be copied.
   * @param copyRange Defines the byte range for this part. Optional.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload.
   *
   * @return The etag of the uploaded part.
   *
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
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CopyPartResult> uploadPartCopy(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_COPY_SOURCE) CopySource copySource,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_RANGE, required = false) HttpRange copyRange,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH,
          required = false) List<String> noneMatch,
      @RequestParam String uploadId,
      @RequestParam String partNumber,
      @RequestHeader HttpHeaders httpHeaders) {
    //TODO: needs modified-since handling, see API
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyPartNumberLimits(partNumber);
    S3ObjectMetadata s3ObjectMetadata =
        objectService.verifyObjectExists(copySource.bucket(), copySource.key());
    objectService.verifyObjectMatchingForCopy(match, noneMatch, s3ObjectMetadata);

    CopyPartResult result = multipartService.copyPart(copySource.bucket(),
        copySource.key(),
        copyRange,
        partNumber,
        bucketName,
        key.key(),
        uploadId,
        parseEncryptionHeaders(httpHeaders)
    );

    return ResponseEntity.ok(result);
  }

  /**
   * Initiates a multipart upload accepting encryption headers.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @PostMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOADS
      },
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<InitiateMultipartUploadResult> createMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader HttpHeaders httpHeaders) {
    bucketService.verifyBucketExists(bucketName);

    String uploadId = UUID.randomUUID().toString();
    InitiateMultipartUploadResult result =
        multipartService.prepareMultipartUpload(bucketName, key.key(),
            contentType, parseStoreHeaders(httpHeaders), uploadId,
            DEFAULT_OWNER, DEFAULT_OWNER, parseUserMetadata(httpHeaders),
            parseEncryptionHeaders(httpHeaders));

    return ResponseEntity.ok(result);
  }

  /**
   * Adds an object to a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   *
   * @return {@link CompleteMultipartUploadResult}
   */
  @PostMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID
      },
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId,
      @RequestBody CompleteMultipartUpload upload,
      HttpServletRequest request,
      @RequestHeader HttpHeaders httpHeaders) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartUploadExists(uploadId);
    multipartService.verifyMultipartParts(bucketName, key.key(), uploadId, upload.parts());
    String objectName = key.key();
    String locationWithEncodedKey = request
        .getRequestURL()
        .toString()
        .replace(objectName, SdkHttpUtils.urlEncode(objectName));

    CompleteMultipartUploadResult result = multipartService.completeMultipartUpload(bucketName,
        key.key(),
        uploadId,
        upload.parts(),
        parseEncryptionHeaders(httpHeaders),
        locationWithEncodedKey);

    return ResponseEntity.ok(result);
  }
}
