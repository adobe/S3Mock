/*
 *  Copyright 2017-2022 Adobe.
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
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID;
import static com.adobe.testing.s3mock.util.HeaderUtil.getUserMetadata;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
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
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.MultipartService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Handles requests related to parts.
 */
@CrossOrigin(origins = "*")
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
  @RequestMapping(
      value = {
          //AWS SDK V2 pattern
          "/{bucketName:.+}",
          //AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          UPLOADS
      },
      method = RequestMethod.GET,
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
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID,
          NOT_LIFECYCLE
      },
      method = RequestMethod.DELETE,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<Void> abortMultipartUpload(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId) {
    bucketService.verifyBucketExists(bucketName);

    multipartService.abortMultipartUpload(bucketName, key.getKey(), uploadId);
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
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID
      },
      method = RequestMethod.GET,
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
        multipartService.getMultipartUploadParts(bucketName, key.getKey(), uploadId);
    return ResponseEntity.ok(result);
  }


  /**
   * Adds an object to a bucket accepting encryption headers.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload.
   * @param encryption Defines the encryption mode.
   * @param kmsKeyId Defines the KMS key id.
   *
   * @return the etag of the uploaded part.
   *
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID,
          PART_NUMBER
      },
      headers = {
          NOT_X_AMZ_COPY_SOURCE,
          NOT_X_AMZ_COPY_SOURCE_RANGE
      },
      method = RequestMethod.PUT
  )
  public ResponseEntity<Void> uploadPart(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId,
      @RequestParam String partNumber,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) String kmsKeyId,
      @RequestHeader(value = X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      InputStream inputStream) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyPartNumberLimits(partNumber);

    String etag = multipartService.putPart(bucketName,
        key.getKey(),
        uploadId,
        partNumber,
        inputStream,
        isV4ChunkedWithSigningEnabled(sha256Header),
        encryption,
        kmsKeyId);

    return ResponseEntity.ok().eTag("\"" + etag + "\"").build();
  }

  /**
   * Uploads a part by copying data from an existing object as data source.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html">API Reference</a>
   *
   * @param copySource References the Objects to be copied.
   * @param copyRange Defines the byte range for this part. Optional.
   * @param encryption The encryption type.
   * @param kmsKeyId The KMS encryption key id.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload.
   *
   * @return The etag of the uploaded part.
   *
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      headers = {
          X_AMZ_COPY_SOURCE,
      },
      params = {
          UPLOAD_ID,
          PART_NUMBER
      },
      method = RequestMethod.PUT,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CopyPartResult> uploadPartCopy(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_COPY_SOURCE) CopySource copySource,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_RANGE, required = false) Range copyRange,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) String kmsKeyId,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH,
          required = false) List<String> noneMatch,
      @RequestParam String uploadId,
      @RequestParam String partNumber) {
    //TODO: needs modified-since handling, see API
    bucketService.verifyBucketExists(bucketName);
    S3ObjectMetadata s3ObjectMetadata =
        objectService.verifyObjectExists(copySource.getBucket(), copySource.getKey());
    objectService.verifyObjectMatchingForCopy(match, noneMatch, s3ObjectMetadata);

    CopyPartResult result = multipartService.copyPart(copySource.getBucket(),
        copySource.getKey(),
        copyRange,
        partNumber,
        bucketName,
        key.getKey(),
        uploadId
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
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOADS
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<InitiateMultipartUploadResult> createMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) String kmsKeyId,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader(value = CONTENT_ENCODING, required = false) String contentEncoding,
      @RequestHeader HttpHeaders httpHeaders) {
    bucketService.verifyBucketExists(bucketName);

    Map<String, String> userMetadata = getUserMetadata(httpHeaders);

    String uploadId = UUID.randomUUID().toString();
    InitiateMultipartUploadResult result =
        multipartService.prepareMultipartUpload(bucketName, key.getKey(),
            contentType, contentEncoding, uploadId,
            DEFAULT_OWNER, DEFAULT_OWNER, userMetadata);

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
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          UPLOAD_ID
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam String uploadId,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) String kmsKeyId,
      @RequestBody CompleteMultipartUpload upload,
      HttpServletRequest request) {
    bucketService.verifyBucketExists(bucketName);
    multipartService.verifyMultipartParts(bucketName, key.getKey(), uploadId, upload.getParts());
    CompleteMultipartUploadResult result = multipartService.completeMultipartUpload(bucketName,
        key.getKey(),
        uploadId,
        upload.getParts(),
        encryption,
        kmsKeyId,
        request.getRequestURL().toString());

    return ResponseEntity.ok(result);
  }
}
