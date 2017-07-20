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

package com.adobe.testing.s3mock;

import static com.adobe.testing.s3mock.util.BetterHeaders.COPY_SOURCE;
import static com.adobe.testing.s3mock.util.BetterHeaders.COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.BetterHeaders.NOT_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.BetterHeaders.NOT_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.BetterHeaders.NOT_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.BetterHeaders.RANGE;
import static com.adobe.testing.s3mock.util.BetterHeaders.SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.BetterHeaders.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID;

import com.adobe.testing.s3mock.domain.BucketContents;
import com.adobe.testing.s3mock.domain.FileStore;
import com.adobe.testing.s3mock.domain.S3Object;
import com.adobe.testing.s3mock.dto.BatchDeleteRequest;
import com.adobe.testing.s3mock.dto.BatchDeleteResponse;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.DeletedObject;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.ObjectRef;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Range;
import com.amazonaws.services.s3.model.StorageClass;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller to handle http requests.
 */
@RestController
class FileStoreController {
  private static final String ANY = "*";

  private static final String RANGES_BYTES = "bytes";

  private static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

  private static final String HEADER_X_AMZ_CONTENT_SHA256 = "x-amz-content-sha256";
  private static final String ABSENT_ENCRYPTION = null;
  private static final String ABSENT_KEY_ID = null;

  private static final Logger LOG = Logger.getLogger(FileStoreController.class);

  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");

  @Autowired
  private FileStore fileStore;

  /**
   * @return a list of all Buckets
   */
  @RequestMapping(value = "/", method = RequestMethod.GET, produces = {
      "application/x-www-form-urlencoded"})
  public @ResponseBody ListAllMyBucketsResult listBuckets() {
    return new ListAllMyBucketsResult(TEST_OWNER, fileStore.listBuckets());
  }

  /**
   * Creates a bucket.
   *
   * @param bucketName name of the bucket that should be created.
   * @return ResponseEntity with Status Code
   */
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.PUT)
  public ResponseEntity<String> putBucket(@PathVariable final String bucketName) {
    try {
      fileStore.createBucket(bucketName);
      return new ResponseEntity<>(HttpStatus.OK);
    } catch (final IOException e) {
      LOG.error("Bucket could not be created!", e);
      return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Operation to determine if a bucket exists.
   *
   * @param bucketName name of the Bucket to be checked.
   * @return ResponseEntity Code 200 OK; 404 Not found.
   */
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.HEAD)
  public ResponseEntity<String> headBucket(@PathVariable final String bucketName) {
    if (fileStore.doesBucketExist(bucketName)) {
      return new ResponseEntity<>(HttpStatus.OK);
    } else {
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
  }

  /**
   * Deletes a specified bucket.
   *
   * @param bucketName name of bucket containing the object.
   * @return ResponseEntity with Status Code 204 if object was successfully deleted; 404 if Not
   * found
   */
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.DELETE)
  public ResponseEntity<String> deleteBucket(@PathVariable final String bucketName) {
    final boolean deleted;

    try {
      deleted = fileStore.deleteBucket(bucketName);
    } catch (final IOException e) {
      LOG.error("Bucket could not be deleted!", e);
      return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    if (deleted) {
      return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    } else {
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
  }

  /**
   * Retrieves metadata from an object without returning the object itself.
   *
   * @param bucketName name of the bucket to look in
   * @param fileName name of the file to look for
   * @return ResponseEntity containing metadata and status
   */
  @RequestMapping(value = "/{bucketName}/{fileName:.+}", method = RequestMethod.HEAD)
  public ResponseEntity<String> headObject(@PathVariable final String bucketName,
      @PathVariable final String fileName) {
    final S3Object s3Object = fileStore.getS3Object(bucketName, fileName);
    if (s3Object != null) {
      final HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setContentLength(Long.valueOf(s3Object.getSize()));
      if (!"".equals(s3Object.getContentType())) {
        responseHeaders.setContentType(MediaType.parseMediaType(s3Object.getContentType()));
      }
      responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
      responseHeaders.setLastModified(s3Object.getLastModified());

      if (s3Object.isEncrypted()) {
        responseHeaders.add(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
            s3Object.getKmsKeyId());
      }

      return new ResponseEntity<>(responseHeaders, HttpStatus.OK);
    } else {
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
  }

  /**
   * Retrieve list of objects of a bucket see http://docs.aws.amazon
   * .com/AmazonS3/latest/API/RESTBucketGET.html
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they starts with prefix
   * @param response {@link HttpServletResponse}
   * @return {@link ListBucketResult} a list of objects in Bucket
   * @throws IOException IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{bucketName}/",
      method = RequestMethod.GET,
      produces = {"application/x-www-form-urlencoded"})
  @ResponseBody
  public ListBucketResult listObjectsInsideBucket(@PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      final HttpServletResponse response) throws IOException {
    try {
      final List<BucketContents> contents = new ArrayList<>();
      final List<S3Object> s3Objects = fileStore.getS3Objects(bucketName, prefix);
      LOG.debug(String.format("Found %s objects in bucket %s", s3Objects.size(), bucketName));
      for (final S3Object s3Object : s3Objects) {
        contents.add(new BucketContents(s3Object.getName(),
            s3Object.getModificationDate(),
            s3Object.getMd5(),
            s3Object.getSize(),
            StorageClass.Standard,
            TEST_OWNER));
      }

      return new ListBucketResult(bucketName, prefix, null, "1000", false, contents, null);
    } catch (final IOException e) {
      LOG.error(String.format("Object(s) could not retrieved from bucket %s", bucketName));
      response.sendError(500, e.getMessage());
    }
    return null;
  }

  /**
   * Adds an object to a bucket.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the File.
   * @param request http servlet request
   * @return ResponseEntity with Status Code and ETag
   */
  @RequestMapping(value = "/{bucketName}/{fileName:.+}", method = RequestMethod.PUT)
  public ResponseEntity<String> putObject(@PathVariable final String bucketName,
      @PathVariable final String fileName,
      final HttpServletRequest request) {
    try (ServletInputStream inputStream = request.getInputStream()) {
      final S3Object s3Object = fileStore.putS3Object(bucketName,
          fileName,
          request.getContentType(),
          inputStream,
          isV4SigningEnabled(request));

      final HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
      responseHeaders.setLastModified(s3Object.getLastModified());
      return new ResponseEntity<>(responseHeaders, HttpStatus.CREATED);
    } catch (final IOException e) {
      LOG.error("Object could not be saved!", e);
      return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private boolean isV4SigningEnabled(final HttpServletRequest request) {
    final String sha256Header = request.getHeader(HEADER_X_AMZ_CONTENT_SHA256);
    return sha256Header != null && !sha256Header.equals(UNSIGNED_PAYLOAD);
  }

  /**
   * Adds an encrypted object to a bucket.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the File.
   * @param encryption The encryption type.
   * @param kmsKeyId The KMS encryption key id.
   * @param request http servlet request.
   *
   * @return {@link ResponseEntity} with Status Code and empty ETag.
   *
   * @throws IOException in case of an error on storing the object.
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      headers = {
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID
      },
      method = RequestMethod.PUT)
  public ResponseEntity<String> putObjectEncrypted(@PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {
    final S3Object s3Object;
    try (ServletInputStream inputStream = request.getInputStream()) {
      s3Object =
          fileStore.putS3ObjectWithKMSEncryption(bucketName,
              fileName,
              request.getContentType(),
              inputStream,
              isV4SigningEnabled(request),
              encryption,
              kmsKeyId);

      final HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
      responseHeaders.setLastModified(s3Object.getLastModified());
      responseHeaders.add(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, kmsKeyId);

      return new ResponseEntity<>(responseHeaders, HttpStatus.CREATED);
    }
  }

  /**
   * Copies an object to another bucket
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
   *
   * @param destinationBucket name of the destination bucket
   * @param destinationFile name of the destination object
   * @param objectRef path to source object
   * @param response response object
   * @return {@link CopyObjectResult}
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{destinationBucket}/{destinationFile:.+}",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          NOT_SERVER_SIDE_ENCRYPTION
      },
      produces = "application/x-www-form-urlencoded; charset=utf-8")
  public @ResponseBody CopyObjectResult copyObject(@PathVariable final String destinationBucket,
      @PathVariable final String destinationFile,
      @RequestHeader(value = COPY_SOURCE) final ObjectRef objectRef,
      final HttpServletResponse response) throws IOException {

    return copyObject(destinationBucket,
        destinationFile,
        objectRef,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        response);
  }

  /**
   * Copies an object encrypted to another bucket
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
   *
   * @param destinationBucket name of the destination bucket
   * @param destinationFile name of the destination object
   * @param objectRef path to source object
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   * @param response response object
   * @return {@link CopyObjectResult}
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{destinationBucket}/{destinationFile:.+}",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          SERVER_SIDE_ENCRYPTION
      },
      produces = "application/x-www-form-urlencoded; charset=utf-8")
  public @ResponseBody CopyObjectResult copyObject(
      @PathVariable final String destinationBucket,
      @PathVariable final String destinationFile,
      @RequestHeader(value = COPY_SOURCE) final ObjectRef objectRef,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      final HttpServletResponse response) throws IOException {

    final CopyObjectResult copyObjectResult =
        fileStore.copyS3ObjectEncrypted(objectRef.getBucket(),
            objectRef.getKey(),
            destinationBucket,
            destinationFile,
            encryption,
            kmsKeyId);

    response.addHeader(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, kmsKeyId);

    if (copyObjectResult == null) {
      response.sendError(404,
          String.format("Could not find source File %s in Bucket %s!",
              objectRef.getBucket(),
              objectRef.getKey()));
    }

    return copyObjectResult;
  }

  /**
   * Returns the File identified by bucketName and fileName
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
   *
   * @param bucketName The Buckets names
   * @param fileName the Files Name
   * @param range byte range
   * @param response response object
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      method = RequestMethod.GET,
      produces = "application/x-www-form-urlencoded")
  public void getObject(@PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestHeader(value = RANGE, required = false) final Range range,
      final HttpServletResponse response) throws IOException {
    final S3Object s3Object = fileStore.getS3Object(bucketName, fileName);

    if (s3Object == null) {
      LOG.error("Object could not be found!");
      response.sendError(404,
          String.format("File %s in Bucket %s couldn't be found!", fileName, bucketName));
    } else if (range != null) {
      getObjectWithRange(response, range, s3Object);
    } else {
      response.setHeader(HttpHeaders.ETAG, "\"" + s3Object.getMd5() + "\"");
      response.setContentType(s3Object.getContentType());
      response.setContentLengthLong(s3Object.getDataFile().length());
      response.setHeader(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES);
      response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ANY);

      try (OutputStream outputStream = response.getOutputStream()) {
        Files.copy(s3Object.getDataFile().toPath(), outputStream);
      }
    }
  }

  /**
   * The DELETE operation removes an object.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
   *
   * @param bucketName name of bucket containing the object.
   * @param objectName name of the object to be deleted.
   * @return ResponseEntity with Status Code 204 if object was successfully deleted.
   */
  @RequestMapping(value = "/{bucketName}/{objectName:.+}", method = RequestMethod.DELETE)
  public ResponseEntity<String> deleteObject(@PathVariable final String bucketName,
      @PathVariable final String objectName) {
    try {
      fileStore.deleteObject(bucketName, objectName);
    } catch (final IOException e) {
      LOG.error("Object could not be deleted!", e);
      return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  /**
   * The batch DELETE operation removes multiple objects.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
   *
   * @param bucketName name of bucket containing the object.
   * @param body The batch delete request.
   * @return The {@link BatchDeleteResponse}
   */
  @RequestMapping(
      value = "/{bucketName}",
      params = "delete",
      method = RequestMethod.POST,
      produces = {"application/x-www-form-urlencoded"})
  public BatchDeleteResponse batchDeleteObjects(@PathVariable final String bucketName,
      @RequestBody final BatchDeleteRequest body) {
    final BatchDeleteResponse response = new BatchDeleteResponse();
    for (final BatchDeleteRequest.ObjectToDelete object : body.getObjectsToDelete()) {
      try {
        fileStore.deleteObject(bucketName, object.getKey());

        final DeletedObject deletedObject = new DeletedObject();
        deletedObject.setKey(object.getKey());
        response.addDeletedObject(deletedObject);
      } catch (final IOException e) {
        LOG.error("Object could not be deleted!", e);
      }
    }

    return response;
  }

  /**
   * Initiates a multipart upload.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the file to upload as multipart.
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      params = "uploads",
      method = RequestMethod.POST,
      produces = "application/x-www-form-urlencoded")
  public InitiateMultipartUploadResult initiateMultipartUpload(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      final HttpServletRequest request) {

    return initiateMultipartUpload(bucketName,
        fileName,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        request);
  }

  /**
   * Initiates a multipart upload accepting encryption headers.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the file to upload as multipart.
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      params = "uploads",
      headers = {
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID
      },
      method = RequestMethod.POST,
      produces = "application/x-www-form-urlencoded")
  public InitiateMultipartUploadResult initiateMultipartUpload(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID) final String kmsKeyId,
      final HttpServletRequest request) {
    final String uploadId = UUID.randomUUID().toString();
    fileStore.prepareMultipartUpload(bucketName, fileName, request.getContentType(), uploadId);

    return new InitiateMultipartUploadResult(bucketName, fileName, uploadId);
  }

  /**
   * Lists all parts a file multipart upload.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the file to upload as multipart.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @return the {@link ListPartsResult}
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      params = {"uploadId", "part-number-marker"},
      method = RequestMethod.GET,
      produces = "application/x-www-form-urlencoded")
  public ListPartsResult multipartListParts(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestParam final String uploadId) {
    return new ListPartsResult(bucketName, fileName, uploadId);
  }

  /**
   * Adds an object to a bucket accepting encryption headers.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the file to upload as multipart.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload.
   * @param encryption Defines the encryption mode.
   * @param kmsKeyId Defines the KMS key id.
   * @param request {@link HttpServletRequest} of this request.
   *
   * @return the etag of the uploaded part.
   *
   * @throws IOException in case of an error.
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      params = {"uploadId", "partNumber"},
      headers = {
          NOT_COPY_SOURCE,
          NOT_COPY_SOURCE_RANGE,
          SERVER_SIDE_ENCRYPTION
      },
      method = RequestMethod.PUT)
  public ResponseEntity<CopyPartResult> putObjectPart(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {

    final String etag = fileStore.putPart(bucketName,
        fileName,
        uploadId,
        partNumber,
        request.getInputStream(),
        isV4SigningEnabled(request));

    final HttpHeaders responseHeaders = new HttpHeaders();
    final String quotedEtag = "\"" + etag + "\"";
    responseHeaders.setETag(quotedEtag);

    return new ResponseEntity<>(responseHeaders, HttpStatus.CREATED);
  }

  /**
   * Adds an object to a bucket.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the file to upload as multipart.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload
   * @param request {@link HttpServletRequest} of this request
   * @return the etag of the uploaded part.
   * @throws IOException in case of an error.
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      params = {"uploadId", "partNumber"},
      headers = {
          NOT_COPY_SOURCE,
          NOT_COPY_SOURCE_RANGE
      },
      method = RequestMethod.PUT)
  public ResponseEntity<CopyPartResult> putObjectPart(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {

    return putObjectPart(bucketName,
        fileName,
        uploadId,
        partNumber,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        request);
  }

  /**
   * Uploads a part by copying data from an existing object as data source.
   *
   * See https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
   *
   * @param copySource References the Objects to be copied.
   * @param copyRange Defines the byte range for this part.
   * @param encryption The encryption type.
   * @param kmsKeyId The KMS encryption key id.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload.
   * @param request {@link HttpServletRequest} of this request.
   *
   * @return The etag of the uploaded part.
   *
   * @throws IOException in case of an error.
   */
  @RequestMapping(
      value = "/{destinationBucket}/{destinationFile:.+}",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          COPY_SOURCE_RANGE,
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID
      })
  public ResponseEntity<CopyPartResult> copyObjectPart(
      @RequestHeader(value = COPY_SOURCE) final ObjectRef copySource,
      @RequestHeader(value = COPY_SOURCE_RANGE) final Range copyRange,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      @PathVariable final String destinationBucket,
      @PathVariable final String destinationFile,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {

    final String partEtag = fileStore.copyPart(copySource.getBucket(),
        copySource.getKey(),
        (int) copyRange.getStart(),
        (int) copyRange.getEnd(),
        isV4SigningEnabled(request),
        partNumber,
        destinationBucket,
        destinationFile,
        uploadId
    );

    return ResponseEntity.ok(CopyPartResult.from(new Date(), "\"" + partEtag + "\""));
  }

  @RequestMapping(
      value = "/{destinationBucket}/{destinationFile:.+}",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          COPY_SOURCE_RANGE,
          NOT_SERVER_SIDE_ENCRYPTION
      })
  public ResponseEntity<CopyPartResult> copyObjectPart(
      @RequestHeader(value = COPY_SOURCE) final ObjectRef copySource,
      @RequestHeader(value = COPY_SOURCE_RANGE) final Range copyRange,
      @PathVariable final String destinationBucket,
      @PathVariable final String destinationFile,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {

    return copyObjectPart(copySource,
        copyRange,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        destinationBucket,
        destinationFile,
        uploadId,
        partNumber,
        request);
  }

  /**
   * Adds an object to a bucket.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the File.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param request {@link HttpServletRequest} of this request
   * @return {@link CompleteMultipartUploadResult}
   * @throws IOException in case of an error.
   */
  @RequestMapping(value = "/{bucketName}/{fileName:.+}", params = {
      "uploadId"}, method = RequestMethod.POST)
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) throws IOException {
    final String eTag =
        fileStore.completeMultipartUpload(bucketName, fileName, uploadId);
    return new ResponseEntity<>(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName,
            fileName, eTag), new HttpHeaders(), HttpStatus.OK);
  }

  /**
   * Adds an object to a bucket.
   *
   * http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param fileName Name of the File.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param request {@link HttpServletRequest} of this request
   * @return {@link CompleteMultipartUploadResult}
   * @throws IOException in case of an error.
   */
  @RequestMapping(
      value = "/{bucketName}/{fileName:.+}",
      headers = {
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID
      },
      params = {"uploadId"},
      method = RequestMethod.POST)
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUploadEncrpyted(
      @PathVariable final String bucketName,
      @PathVariable final String fileName,
      @RequestParam final String uploadId,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {
    final String eTag = fileStore.completeMultipartUpload(bucketName,
        fileName,
        uploadId,
        encryption,
        kmsKeyId);

    return new ResponseEntity<>(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName,
            fileName, eTag), new HttpHeaders(), HttpStatus.OK);
  }

  /**
   * supports range different range ends. eg. if content has 100 bytes, the range request could
   * be: bytes=10-100,
   * 10--1 and 10-200
   *
   * see: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
   *
   * @param response {@link HttpServletResponse}
   * @param range {@link String}
   * @param s3Object {@link S3Object}
   * @throws IOException if invalid range request value
   */
  private void getObjectWithRange(final HttpServletResponse response, final Range range,
      final S3Object s3Object)
      throws IOException {
    final long fileSize = s3Object.getDataFile().length();
    final long bytesToRead = Math.min(fileSize - 1, range.getEnd()) - range.getStart() + 1;

    if (bytesToRead < 0 || fileSize < range.getStart()) {
      response.setStatus(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value());
      response.flushBuffer();
      return;
    }

    response.setStatus(HttpStatus.PARTIAL_CONTENT.value());
    response.setHeader(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES);
    response.setHeader(HttpHeaders.CONTENT_RANGE,
        String.format("bytes %s-%s", range.getStart(), bytesToRead + range.getStart() - 1));
    response.setHeader(HttpHeaders.ETAG, "\"" + s3Object.getMd5() + "\"");
    response.setDateHeader(HttpHeaders.LAST_MODIFIED, s3Object.getLastModified());

    response.setContentType(s3Object.getContentType());
    response.setContentLengthLong(bytesToRead);

    try (OutputStream outputStream = response.getOutputStream()) {
      try (FileInputStream fis = new FileInputStream(s3Object.getDataFile())) {
        fis.skip(range.getStart());
        IOUtils.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
      }
    }
  }
}
