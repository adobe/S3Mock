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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective.METADATA_DIRECTIVE_COPY;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CONTENT_SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.CONTINUATION_TOKEN;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.DELETE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ENCODING_TYPE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LIST_TYPE_V2;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.MAX_KEYS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOAD_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.START_AFTER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.UPLOAD_ID;
import static com.adobe.testing.s3mock.util.MetadataUtil.createUserMetadataHeaders;
import static com.adobe.testing.s3mock.util.MetadataUtil.getUserMetadata;
import static com.adobe.testing.s3mock.util.StringEncoding.decode;
import static com.adobe.testing.s3mock.util.StringEncoding.encode;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.NOT_MODIFIED;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.PRECONDITION_FAILED;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.BatchDeleteRequest;
import com.adobe.testing.s3mock.dto.BatchDeleteResponse;
import com.adobe.testing.s3mock.dto.Bucket;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadRequest;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CompletedPart;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.CopySource;
import com.adobe.testing.s3mock.dto.DeletedS3Object;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.util.AwsChunkedDecodingInputStream;
import com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective;
import com.adobe.testing.s3mock.util.DigestUtil;
import com.adobe.testing.s3mock.util.StringEncoding;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Controller to handle http requests.
 */
@CrossOrigin(origins = "*")
@RequestMapping("${com.adobe.testing.s3mock.contextPath:}")
public class FileStoreController {
  private static final String RANGES_BYTES = "bytes";

  private static final String STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD =
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

  private static final String RESPONSE_HEADER_CONTENT_TYPE = "response-content-type";
  private static final String RESPONSE_HEADER_CONTENT_LANGUAGE = "response-content-language";
  private static final String RESPONSE_HEADER_EXPIRES = "response-expires";
  private static final String RESPONSE_HEADER_CACHE_CONTROL = "response-cache-control";
  private static final String RESPONSE_HEADER_CONTENT_DISPOSITION = "response-content-disposition";
  private static final String RESPONSE_HEADER_CONTENT_ENCODING = "response-content-encoding";

  private static final Logger LOG = LoggerFactory.getLogger(FileStoreController.class);

  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");

  private static final Comparator<String> KEY_COMPARATOR = Comparator.naturalOrder();
  private static final Comparator<S3Object> BUCKET_CONTENTS_COMPARATOR =
      Comparator.comparing(S3Object::getKey, KEY_COMPARATOR);

  private static final MediaType FALLBACK_MEDIA_TYPE = new MediaType("binary", "octet-stream");

  private static final Long MINIMUM_PART_SIZE = 5L * 1024L * 1024L;

  private final Map<String, String> fileStorePagingStateCache = new ConcurrentHashMap<>();
  private final FileStore fileStore;

  public FileStoreController(FileStore fileStore) {
    this.fileStore = fileStore;
  }

  //================================================================================================
  // /
  //================================================================================================

  /**
   * List all existing buckets.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html</p>
   *
   * @return List of all Buckets
   */
  @RequestMapping(
      value = "/",
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListAllMyBucketsResult> listBuckets() {
    return ResponseEntity.ok(new ListAllMyBucketsResult(TEST_OWNER, fileStore.listBuckets()));
  }

  //================================================================================================
  // /{bucketName:.+}
  //================================================================================================

  /**
   * Create a bucket if the name matches a simplified version of the bucket naming rules.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html</p>
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html</p>
   *
   * @param bucketName name of the bucket that should be created.
   *
   * @return 200 OK if creation was successful.
   */
  @RequestMapping(
      value = "/{bucketName}",
      method = RequestMethod.PUT
  )
  public ResponseEntity<String> createBucket(@PathVariable final String bucketName) {
    try {
      fileStore.createBucket(bucketName);
      return ResponseEntity.ok().build();
    } catch (RuntimeException e) {
      LOG.error("Bucket could not be created!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Check if a bucket exists.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html</p>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 200 if it exists; 404 if not found.
   */
  @RequestMapping(
      value = "/{bucketName}",
      method = RequestMethod.HEAD
  )
  public ResponseEntity<Void> headBucket(@PathVariable final String bucketName) {
    if (fileStore.doesBucketExist(bucketName)) {
      return ResponseEntity.ok().build();
    } else {
      return ResponseEntity.notFound().build();
    }
  }

  /**
   * Delete a bucket.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html</p>
   *
   * @param bucketName name of the Bucket.
   *
   * @return 204 if Bucket was deleted; 404 if not found
   */
  @RequestMapping(
      value = "/{bucketName}",
      method = RequestMethod.DELETE
  )
  public ResponseEntity<String> deleteBucket(@PathVariable final String bucketName) {
    verifyBucketExistence(bucketName);

    final boolean deleted;

    try {
      if (!fileStore.getS3Objects(bucketName, null).isEmpty()) {
        throw new S3Exception(CONFLICT.value(), "BucketNotEmpty",
            "The bucket you tried to delete is not empty.");
      }
      deleted = fileStore.deleteBucket(bucketName);
    } catch (final IOException e) {
      LOG.error("Bucket could not be deleted!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }

    if (deleted) {
      return ResponseEntity.noContent().build();
    } else {
      return ResponseEntity.notFound().build();
    }
  }

  /**
   * Retrieve list of objects of a bucket.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html</p>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they starts with prefix
   * @param encodingType whether to use URL encoding (encodingtype="url") or not
   *
   * @return {@link ListBucketResult} a list of objects in Bucket
   * @deprecated Long since replaced by ListObjectsV2, {@see #listObjectsInsideBucketV2}
   */
  @RequestMapping(
      params = {
          NOT_UPLOADS
      },
      value = "/{bucketName}",
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  @Deprecated
  public ResponseEntity<ListBucketResult> listObjectsInsideBucket(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(required = false) final String marker,
      @RequestParam(name = ENCODING_TYPE, required = false) final String encodingType,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000",
          required = false) final Integer maxKeys) {
    verifyBucketExistence(bucketName);
    if (maxKeys < 0) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "maxKeys should be non-negative");
    }
    if (isNotEmpty(encodingType) && !"url".equals(encodingType)) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "encodingtype can only be none or 'url'");
    }

    final boolean useUrlEncoding = Objects.equals("url", encodingType);

    try {
      List<S3Object> contents = getBucketContents(bucketName, prefix);
      contents = filterBucketContentsBy(contents, marker);

      boolean isTruncated = false;
      String nextMarker = null;

      Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents);
      contents = filterBucketContentsBy(contents, commonPrefixes);
      if (maxKeys < contents.size()) {
        contents = contents.subList(0, maxKeys);
        isTruncated = true;
        if (maxKeys > 0) {
          nextMarker = contents.get(maxKeys - 1).getKey();
        }
      }

      String returnPrefix = prefix;
      Set<String> returnCommonPrefixes = commonPrefixes;

      if (useUrlEncoding) {
        contents = applyUrlEncoding(contents);
        returnPrefix = isNotBlank(prefix) ? encode(prefix) : prefix;
        returnCommonPrefixes = applyUrlEncoding(commonPrefixes);
      }

      return ResponseEntity.ok(
          new ListBucketResult(bucketName, returnPrefix, marker, maxKeys, isTruncated,
              encodingType, nextMarker, contents, returnCommonPrefixes));
    } catch (final IOException e) {
      LOG.error("Object(s) could not retrieved from bucket {}", bucketName, e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Retrieve list of objects of a bucket.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html</p>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they start with prefix
   * @param startAfter {@link String} return key names after a specific object key in your key
   *     space
   * @param maxKeys {@link Integer} set maximum number of keys to be returned
   * @param continuationToken {@link String} pagination token returned by previous request
   *
   * @return {@link ListBucketResultV2} a list of objects in Bucket
   */
  @RequestMapping(value = "/{bucketName}",
      params = {
          LIST_TYPE_V2
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListBucketResultV2> listObjectsInsideBucketV2(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) final String encodingtype,
      @RequestParam(name = START_AFTER, required = false) final String startAfter,
      @RequestParam(name = MAX_KEYS,
          defaultValue = "1000", required = false) final Integer maxKeys,
      @RequestParam(name = CONTINUATION_TOKEN, required = false) final String continuationToken) {
    if (isNotEmpty(encodingtype) && !"url".equals(encodingtype)) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "encodingtype can only be none or 'url'");
    }

    final boolean useUrlEncoding = Objects.equals("url", encodingtype);

    verifyBucketExistence(bucketName);
    try {
      List<S3Object> contents = getBucketContents(bucketName, prefix);
      String nextContinuationToken = null;
      boolean isTruncated = false;

      /*
        Start-after is valid only in first request.
        If the response is truncated,
        you can specify this parameter along with the continuation-token parameter,
        and then Amazon S3 ignores this parameter.
       */
      if (continuationToken != null) {
        final String continueAfter = fileStorePagingStateCache.get(continuationToken);
        contents = filterBucketContentsBy(contents, continueAfter);
        fileStorePagingStateCache.remove(continuationToken);
      } else {
        contents = filterBucketContentsBy(contents, startAfter);
      }

      Set<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents);
      contents = filterBucketContentsBy(contents, commonPrefixes);

      if (contents.size() > maxKeys) {
        isTruncated = true;
        nextContinuationToken = UUID.randomUUID().toString();
        contents = contents.subList(0, maxKeys);
        fileStorePagingStateCache.put(nextContinuationToken,
            contents.get(maxKeys - 1).getKey());
      }

      String returnPrefix = prefix;
      String returnStartAfter = startAfter;
      Set<String> returnCommonPrefixes = commonPrefixes;

      if (useUrlEncoding) {
        contents = applyUrlEncoding(contents);
        returnPrefix = isNotBlank(prefix) ? encode(prefix) : prefix;
        returnStartAfter = isNotBlank(startAfter) ? encode(startAfter) : startAfter;
        returnCommonPrefixes = applyUrlEncoding(commonPrefixes);
      }

      return ResponseEntity.ok(new ListBucketResultV2(bucketName, returnPrefix, maxKeys,
          isTruncated, contents, returnCommonPrefixes,
          continuationToken, String.valueOf(contents.size()),
          nextContinuationToken, returnStartAfter, encodingtype));
    } catch (final IOException e) {
      LOG.error("Object(s) could not retrieved from bucket {}", bucketName, e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Lists all in-progress multipart uploads.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html</p>
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
          "/{bucketName}",
          //AWS SDK V1 pattern
          "/{bucketName}/"
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
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam final String uploads) {
    verifyBucketExistence(bucketName);

    final List<MultipartUpload> multipartUploads =
        fileStore.listMultipartUploads(bucketName).stream()
            .filter(m -> isBlank(prefix) || m.getKey().startsWith(prefix))
            .map(m -> new MultipartUpload(decode(m.getKey()), m.getUploadId(),
                m.getOwner(), m.getInitiator(), m.getInitiated()))
            .collect(Collectors.toList());

    // the result contains all uploads, use some common value as default
    final int maxUploads = Math.max(1000, multipartUploads.size());
    final boolean isTruncated = false;
    final String uploadIdMarker = null;
    final String nextUploadIdMarker = null;
    final String keyMarker = null;
    final String nextKeyMarker = null;

    // delimiter / prefix search not supported
    final String delimiter = null;
    final List<String> commonPrefixes = Collections.emptyList();

    return ResponseEntity.ok(
        new ListMultipartUploadsResult(bucketName, keyMarker, delimiter, prefix, uploadIdMarker,
            maxUploads, isTruncated, nextKeyMarker, nextUploadIdMarker, multipartUploads,
            commonPrefixes));
  }

  /**
   * The batch DELETE operation removes multiple objects.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html</p>
   *
   * @param bucketName name of bucket containing the object.
   * @param body The batch delete request.
   *
   * @return The {@link BatchDeleteResponse}
   */
  @RequestMapping(
      value = "/{bucketName}",
      params = {
          DELETE
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<BatchDeleteResponse> batchDeleteObjects(
      @PathVariable final String bucketName,
      @RequestBody final BatchDeleteRequest body) {
    verifyBucketExistence(bucketName);
    final BatchDeleteResponse response = new BatchDeleteResponse();
    for (final S3ObjectIdentifier object : body.getObjectsToDelete()) {
      try {
        if (fileStore.deleteObject(bucketName, encode(object.getKey()))) {
          response.addDeletedObject(DeletedS3Object.from(object));
        } else {
          //TODO: There may be different error reasons than a non-existent key.
          response.addError(
              new com.adobe.testing.s3mock.dto.Error("NoSuchKey",
                  object.getKey(),
                  "The specified key does not exist.",
                  object.getVersionId()));
        }
      } catch (final IOException e) {
        response.addError(
            new com.adobe.testing.s3mock.dto.Error("InternalError",
                object.getKey(),
                "We encountered an internal error. Please try again.",
                object.getVersionId()));
        LOG.error("Object could not be deleted!", e);
      }
    }

    return ResponseEntity.ok(response);
  }

  //================================================================================================
  // /{bucketName}/**
  //================================================================================================

  /**
   * Retrieves metadata from an object without returning the object itself.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html</p>
   *
   * @param bucketName name of the bucket to look in
   *
   * @return 200 with object metadata headers, 404 if not found.
   */
  @RequestMapping(
      value = "/{bucketName}/**",
      method = RequestMethod.HEAD
  )
  public ResponseEntity<String> headObject(@PathVariable final String bucketName,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);

    final com.adobe.testing.s3mock.store.S3Object
        s3Object = fileStore.getS3Object(bucketName, filename);
    if (s3Object != null) {
      return ResponseEntity.ok()
          .headers(headers -> headers.setAll(createUserMetadataHeaders(s3Object)))
          .headers(headers -> {
            if (s3Object.isEncrypted()) {
              headers.set(X_AMZ_SERVER_SIDE_ENCRYPTION, s3Object.getKmsEncryption());
              headers.set(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, s3Object.getKmsKeyId());
            }
          })
          .contentType(parseMediaType(s3Object.getContentType()))
          .eTag("\"" + s3Object.getEtag() + "\"")
          .contentLength(Long.parseLong(s3Object.getSize()))
          .lastModified(s3Object.getLastModified())
          .build();
    } else {
      return ResponseEntity.status(NOT_FOUND).build();
    }
  }

  /**
   * The DELETE operation removes an object.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html</p>
   *
   * @param bucketName name of bucket containing the object.
   *
   * @return ResponseEntity with Status Code 204 if object was successfully deleted.
   */
  @RequestMapping(
      value = "/{bucketName}/**",
      method = RequestMethod.DELETE
  )
  public ResponseEntity<String> deleteObject(@PathVariable final String bucketName,
      final HttpServletRequest request) {
    final String filename = filenameFrom(bucketName, request);
    verifyBucketExistence(bucketName);

    try {
      fileStore.deleteObject(bucketName, filename);
    } catch (final IOException e) {
      LOG.error("Object could not be deleted!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }

    return ResponseEntity.noContent().build();
  }

  /**
   * Aborts a multipart upload for a given uploadId.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   */
  @RequestMapping(
      value = "/{bucketName}/**",
      params = {
          UPLOAD_ID
      },
      method = RequestMethod.DELETE,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<Void> abortMultipartUpload(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    fileStore.abortMultipartUpload(bucketName, filename, uploadId);
    return ResponseEntity.noContent().build();
  }

  /**
   * Returns the File identified by bucketName and fileName.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html</p>
   *
   * @param bucketName The Buckets names
   * @param range byte range
   *
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{bucketName}/**",
      params = {
          NOT_UPLOADS,
          NOT_UPLOAD_ID,
          NOT_TAGGING
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<StreamingResponseBody> getObject(@PathVariable final String bucketName,
      @RequestHeader(value = RANGE, required = false) final Range range,
      @RequestHeader(value = IF_MATCH, required = false) final List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) final List<String> noMatch,
      final HttpServletRequest request) throws IOException {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final com.adobe.testing.s3mock.store.S3Object
        s3Object = verifyObjectExistence(bucketName, filename);

    verifyObjectMatching(match, noMatch, s3Object.getEtag());

    if (range != null) {
      return getObjectWithRange(range, s3Object);
    }

    return ResponseEntity
        .ok()
        .eTag("\"" + s3Object.getEtag() + "\"")
        .header(HttpHeaders.CONTENT_ENCODING, s3Object.getContentEncoding())
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .headers(headers -> headers.setAll(createUserMetadataHeaders(s3Object)))
        .headers(headers -> {
          if (s3Object.isEncrypted()) {
            headers.set(X_AMZ_SERVER_SIDE_ENCRYPTION, s3Object.getKmsEncryption());
            headers.set(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, s3Object.getKmsKeyId());
          }
        })
        .lastModified(s3Object.getLastModified())
        .contentLength(s3Object.getDataFile().length())
        .contentType(parseMediaType(s3Object.getContentType()))
        .headers(headers -> headers.setAll(addOverrideHeaders(request.getQueryString())))
        .body(outputStream -> Files.copy(s3Object.getDataFile().toPath(), outputStream));
  }

  /**
   * Returns the tags identified by bucketName and fileName.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html</p>
   *
   * @param bucketName The Bucket's name
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {
          TAGGING
      },
      method = RequestMethod.GET,
      produces = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Tagging> getObjectTagging(@PathVariable final String bucketName,
      final HttpServletRequest request) {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final com.adobe.testing.s3mock.store.S3Object
        s3Object = verifyObjectExistence(bucketName, filename);

    final List<Tag> tagList = new ArrayList<>(s3Object.getTags());
    final Tagging result = new Tagging(tagList);

    return ResponseEntity
        .ok()
        .eTag("\"" + s3Object.getEtag() + "\"")
        .lastModified(s3Object.getLastModified())
        .body(result);
  }

  /**
   * Lists all parts a file multipart upload.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   *
   * @return the {@link ListPartsResult}
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {
          UPLOAD_ID
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListPartsResult> multipartListParts(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);
    verifyMultipartUploadExists(uploadId);

    final List<Part> parts = fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    return ResponseEntity.ok(new ListPartsResult(bucketName, filename, uploadId, parts));
  }

  /**
   * Sets tags for a file identified by bucketName and fileName.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html</p>
   *
   * @param bucketName The Bucket's name
   * @param body Tagging object
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {
          TAGGING
      },
      method = RequestMethod.PUT
  )
  public ResponseEntity<String> putObjectTagging(@PathVariable final String bucketName,
      @RequestBody final Tagging body,
      final HttpServletRequest request) {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final com.adobe.testing.s3mock.store.S3Object
        s3Object = verifyObjectExistence(bucketName, filename);

    try {
      fileStore.setObjectTags(bucketName, filename, body.getTagSet());
      return ResponseEntity
          .ok()
          .eTag("\"" + s3Object.getEtag() + "\"")
          .lastModified(s3Object.getLastModified())
          .build();
    } catch (final IOException e) {
      LOG.error("Tags could not be set!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Adds an object to a bucket accepting encryption headers.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
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
      value = "/{bucketName:.+}/**",
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
  public ResponseEntity<Void> putObjectPart(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, required = false)
      final String kmsKeyId,
      @RequestHeader(value = X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);
    verifyPartNumberLimits(partNumber);

    final String filename = filenameFrom(bucketName, request);

    final String etag = fileStore.putPart(bucketName,
        filename,
        uploadId,
        partNumber,
        request.getInputStream(),
        isV4ChunkedWithSigningEnabled(sha256Header));

    return ResponseEntity.ok().eTag("\"" + etag + "\"").build();
  }

  /**
   * Uploads a part by copying data from an existing object as data source.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html</p>
   *
   * @param copySource References the Objects to be copied.
   * @param copyRange Defines the byte range for this part. Optional.
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
      value = "/{bucketName}/**",
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
  public ResponseEntity<CopyPartResult> copyObjectPart(
      @RequestHeader(value = X_AMZ_COPY_SOURCE) final CopySource copySource,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_RANGE, required = false) final Range copyRange,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) final String kmsKeyId,
      @PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);

    final String destinationFile = filenameFrom(bucketName, request);
    verifyObjectExistence(copySource.getBucket(), copySource.getKey());
    final String partEtag = fileStore.copyPart(copySource.getBucket(),
        copySource.getKey(),
        copyRange,
        partNumber,
        bucketName,
        destinationFile,
        uploadId
    );

    return ResponseEntity.ok(CopyPartResult.from(new Date(), "\"" + partEtag + "\""));
  }

  /**
   * Adds an object to a bucket.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param encryption The encryption type.
   * @param kmsKeyId The KMS encryption key id.
   * @param request http servlet request.
   *
   * @return {@link ResponseEntity} with Status Code and empty ETag.
   *
   * @throws IOException in case of an error on storing the object.
   */
  @RequestMapping(
      params = {
          NOT_UPLOAD_ID,
          NOT_TAGGING
      },
      headers = {
          NOT_X_AMZ_COPY_SOURCE
      },
      value = "/{bucketName:.+}/**",
      method = RequestMethod.PUT
  )
  public ResponseEntity<String> putObject(@PathVariable final String bucketName,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, required = false)
      final String kmsKeyId,
      @RequestHeader(name = X_AMZ_TAGGING, required = false) final List<Tag> tags,
      @RequestHeader(value = CONTENT_ENCODING, required = false) String contentEncoding,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader(value = CONTENT_MD5, required = false) String contentMd5,
      @RequestHeader(value = X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    final com.adobe.testing.s3mock.store.S3Object s3Object;
    try (final ServletInputStream inputStream = request.getInputStream()) {
      InputStream stream = verifyMd5(inputStream, contentMd5, sha256Header);
      final Map<String, String> userMetadata = getUserMetadata(request);
      s3Object =
          fileStore.putS3Object(bucketName,
              filename,
              parseMediaType(contentType).toString(),
              contentEncoding,
              stream,
              isV4ChunkedWithSigningEnabled(sha256Header),
              userMetadata,
              encryption,
              kmsKeyId);

      fileStore.setObjectTags(bucketName, filename, tags);

      return ResponseEntity
          .ok()
          .eTag("\"" + s3Object.getEtag() + "\"")
          .lastModified(s3Object.getLastModified())
          .header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
          .build();
    } catch (final IOException | NoSuchAlgorithmException e) {
      LOG.error("Object could not be uploaded!", e);
      throw new S3Exception(INTERNAL_SERVER_ERROR.value(), "InternalServerError",
          "Error persisting object.");
    }
  }

  private static InputStream verifyMd5(InputStream inputStream, String contentMd5,
      String sha256Header)
      throws IOException, NoSuchAlgorithmException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    copyTo(inputStream, byteArrayOutputStream);

    InputStream stream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    try {
      if (isV4ChunkedWithSigningEnabled(sha256Header)) {
        stream = new AwsChunkedDecodingInputStream(stream);
      }
      verifyMd5(stream, contentMd5);
    } finally {
      IOUtils.closeQuietly(stream);
    }
    return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }

  private static void verifyMd5(InputStream inputStream, String contentMd5)
      throws NoSuchAlgorithmException, IOException {
    if (contentMd5 != null) {
      String md5 = DigestUtil.getBase64Digest(inputStream);
      if (!md5.equals(contentMd5)) {
        LOG.error("Content-MD5 {} does not match object md5 {}", contentMd5, md5);
        throw new S3Exception(BAD_REQUEST.value(), "BadRequest",
            "Content-MD5 does not match object md5");
      }
    }
  }

  /**
   * Replace with InputStream.transferTo() once we update to Java 9+
   */
  static void copyTo(InputStream source, OutputStream target) throws IOException {
    byte[] buf = new byte[8192];
    int length;
    while ((length = source.read(buf)) > 0) {
      target.write(buf, 0, length);
    }
  }

  /**
   * Copies an object to another bucket.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html</p>
   *
   * @param bucketName name of the destination bucket
   * @param copySource path to source object
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   *
   * @return {@link CopyObjectResult}
   *
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{bucketName}/**",
      headers = {
          X_AMZ_COPY_SOURCE
      },
      params = {
          NOT_UPLOAD_ID
      },
      method = RequestMethod.PUT,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CopyObjectResult> copyObject(@PathVariable final String bucketName,
      @RequestHeader(value = X_AMZ_COPY_SOURCE) final CopySource copySource,
      @RequestHeader(value = X_AMZ_METADATA_DIRECTIVE,
          defaultValue = METADATA_DIRECTIVE_COPY) final MetadataDirective metadataDirective,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);
    verifyObjectExistence(copySource.getBucket(), copySource.getKey());
    final String destinationFile = filenameFrom(bucketName, request);

    final CopyObjectResult copyObjectResult;
    if (MetadataDirective.REPLACE == metadataDirective) {
      copyObjectResult = fileStore.copyS3ObjectEncrypted(copySource.getBucket(),
          copySource.getKey(),
          bucketName,
          destinationFile,
          encryption,
          kmsKeyId,
          getUserMetadata(request));
    } else {
      copyObjectResult = fileStore.copyS3ObjectEncrypted(copySource.getBucket(),
          copySource.getKey(),
          bucketName,
          destinationFile,
          encryption,
          kmsKeyId);
    }

    if (copyObjectResult == null) {
      return ResponseEntity.notFound().header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
          .build();
    }
    return ResponseEntity.ok().header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
        .body(copyObjectResult);
  }

  /**
   * Initiates a multipart upload accepting encryption headers.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {
          UPLOADS
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<InitiateMultipartUploadResult> initiateMultipartUpload(
      @PathVariable final String bucketName,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, required = false)
      final String kmsKeyId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    final Map<String, String> userMetadata = getUserMetadata(request);

    final String uploadId = UUID.randomUUID().toString();
    fileStore.prepareMultipartUpload(bucketName, filename,
        parseMediaType(request.getContentType()).toString(),
        request.getHeader(HttpHeaders.CONTENT_ENCODING), uploadId,
        TEST_OWNER, TEST_OWNER, userMetadata);

    return ResponseEntity.ok(
        new InitiateMultipartUploadResult(bucketName, decode(filename), uploadId));
  }

  /**
   * Adds an object to a bucket.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param request {@link HttpServletRequest} of this request.
   *
   * @return {@link CompleteMultipartUploadResult}
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {
          UPLOAD_ID
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, required = false)
      final String kmsKeyId,
      @RequestBody final CompleteMultipartUploadRequest requestBody,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);
    validateMultipartParts(bucketName, filename, uploadId, requestBody.getParts());
    final String eTag = fileStore.completeMultipartUpload(bucketName,
        filename,
        uploadId,
        requestBody.getParts(),
        encryption,
        kmsKeyId);

    return ResponseEntity.ok(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName,
            filename, eTag));
  }

  /**
   * supports range different range ends. eg. if content has 100 bytes, the range request could be:
   * bytes=10-100, 10--1 and 10-200
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html</p>
   *
   * @param range {@link String}
   * @param s3Object {@link com.adobe.testing.s3mock.store.S3Object}
   */
  private ResponseEntity<StreamingResponseBody> getObjectWithRange(final Range range,
      final com.adobe.testing.s3mock.store.S3Object s3Object) {
    final long fileSize = s3Object.getDataFile().length();
    final long bytesToRead = Math.min(fileSize - 1, range.getEnd()) - range.getStart() + 1;

    if (bytesToRead < 0 || fileSize < range.getStart()) {
      return ResponseEntity.status(REQUESTED_RANGE_NOT_SATISFIABLE.value()).build();
    }

    return ResponseEntity
        .status(PARTIAL_CONTENT.value())
        .headers(headers -> headers.setAll(createUserMetadataHeaders(s3Object)))
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .header(HttpHeaders.CONTENT_RANGE,
            String.format("bytes %s-%s/%s",
                range.getStart(), bytesToRead + range.getStart() - 1, s3Object.getSize()))
        .eTag("\"" + s3Object.getEtag() + "\"")
        .contentType(parseMediaType(s3Object.getContentType()))
        .lastModified(s3Object.getLastModified())
        .contentLength(bytesToRead)
        .body(outputStream -> {
          try (final FileInputStream fis = new FileInputStream(s3Object.getDataFile())) {
            fis.skip(range.getStart());
            IOUtils.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
          }
        });
  }

  private static String filenameFrom(final String bucketName, final HttpServletRequest request) {
    final String requestUri = request.getRequestURI();
    return encode(
        decode(requestUri.substring(requestUri.indexOf(bucketName) + bucketName.length() + 1))
    );
  }

  /**
   * Collapse all bucket elements with keys starting with some prefix up to the given delimiter into
   * one prefix entry. Collapsed elements are removed from the contents list.
   *
   * @param queryPrefix the key prefix as specified in the list request
   * @param delimiter the delimiter used to separate a prefix from the rest of the object name
   * @param contents the contents list
   *
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html">
   *     List Objects API Specification</a>
   */
  static Set<String> collapseCommonPrefixes(final String queryPrefix, final String delimiter,
      final List<S3Object> contents) {
    final Set<String> commonPrefixes = new HashSet<>();
    if (isEmpty(delimiter)) {
      return commonPrefixes;
    }

    final String normalizedQueryPrefix = queryPrefix == null ? "" : queryPrefix;

    for (S3Object c : contents) {
      final String key = c.getKey();
      if (key.startsWith(normalizedQueryPrefix)) {
        final int delimiterIndex = key.indexOf(delimiter, normalizedQueryPrefix.length());
        if (delimiterIndex > 0) {
          commonPrefixes.add(key.substring(0, delimiterIndex + delimiter.length()));
        }
      }
    }
    return commonPrefixes;
  }

  private List<S3Object> applyUrlEncoding(final List<S3Object> contents) {
    return contents.stream().map(c -> new S3Object(encode(c.getKey()),
        c.getLastModified(), c.getEtag(), c.getSize(), c.getStorageClass(), c.getOwner())).collect(
        Collectors.toList());
  }

  private Set<String> applyUrlEncoding(final Set<String> contents) {
    return contents.stream().map(StringEncoding::encode).collect(Collectors.toSet());
  }

  static List<S3Object> filterBucketContentsBy(List<S3Object> contents,
      String startAfter) {
    if (isNotEmpty(startAfter)) {
      return contents
          .stream()
          .filter(p -> KEY_COMPARATOR.compare(p.getKey(), startAfter) > 0)
          .collect(Collectors.toList());
    } else {
      return contents;
    }
  }

  static List<S3Object> filterBucketContentsBy(List<S3Object> contents,
      Set<String> commonPrefixes) {
    if (commonPrefixes != null && !commonPrefixes.isEmpty()) {
      return contents
          .stream()
          .filter(c -> commonPrefixes
              .stream()
              .noneMatch(p -> c.getKey().startsWith(p))
          )
          .collect(Collectors.toList());
    } else {
      return contents;
    }
  }

  private List<S3Object> getBucketContents(final String bucketName,
      final String prefix) throws IOException {
    final String encodedPrefix = null != prefix ? encode(prefix) : null;

    final List<com.adobe.testing.s3mock.store.S3Object> s3Objects =
        fileStore.getS3Objects(bucketName, encodedPrefix);

    LOG.debug(String.format("Found %s objects in bucket %s", s3Objects.size(), bucketName));
    return s3Objects.stream().map(s3Object -> new S3Object(
            decode(s3Object.getName()),
            s3Object.getModificationDate(), s3Object.getEtag(),
            s3Object.getSize(), StorageClass.STANDARD, TEST_OWNER))
        // List Objects results are expected to be sorted by key
        .sorted(BUCKET_CONTENTS_COMPARATOR)
        .collect(Collectors.toList());
  }

  private static boolean isV4ChunkedWithSigningEnabled(final String sha256Header) {
    return sha256Header != null && sha256Header.equals(STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD);
  }

  private Map<String, String> addOverrideHeaders(final String query) {
    if (isNotBlank(query)) {
      return Arrays.stream(query.split("&"))
          .filter(param -> isNotBlank(mapHeaderName(decode(substringBefore(param, "=")))))
          .collect(Collectors.toMap(
              (param) -> mapHeaderName(decode(substringBefore(param, "="))),
              (param) -> decode(substringAfter(param, "="))));
    }
    return Collections.emptyMap();
  }

  private String mapHeaderName(final String name) {
    switch (name) {
      case RESPONSE_HEADER_CACHE_CONTROL:
        return HttpHeaders.CACHE_CONTROL;
      case RESPONSE_HEADER_CONTENT_DISPOSITION:
        return HttpHeaders.CONTENT_DISPOSITION;
      case RESPONSE_HEADER_CONTENT_ENCODING:
        return HttpHeaders.CONTENT_ENCODING;
      case RESPONSE_HEADER_CONTENT_LANGUAGE:
        return HttpHeaders.CONTENT_LANGUAGE;
      case RESPONSE_HEADER_CONTENT_TYPE:
        return HttpHeaders.CONTENT_TYPE;
      case RESPONSE_HEADER_EXPIRES:
        return HttpHeaders.EXPIRES;
      default:
        // Only the above header overrides are supported by S3
        return null;
    }
  }

  private void verifyObjectMatching(
      final List<String> match, final List<String> noneMatch, final String etag) {
    if (match != null && !match.contains(etag)) {
      throw new S3Exception(PRECONDITION_FAILED.value(),
          "PreconditionFailed", "Precondition Failed");
    }
    if (noneMatch != null && noneMatch.contains(etag)) {
      throw new S3Exception(NOT_MODIFIED.value(), "NotModified", "Not Modified");
    }
  }

  private com.adobe.testing.s3mock.store.S3Object verifyObjectExistence(final String bucketName,
      final String filename) {
    final com.adobe.testing.s3mock.store.S3Object s3Object =
        fileStore.getS3Object(bucketName, filename);
    if (s3Object == null) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchKey", "The specified key does not exist.");
    }
    return s3Object;
  }

  private void verifyBucketExistence(final String bucketName) {
    final Bucket bucket = fileStore.getBucket(bucketName);
    if (bucket == null) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchBucket",
          "The specified bucket does not exist.");
    }
  }

  private void verifyPartNumberLimits(final String partNumberString) {
    final int partNumber;
    try {
      partNumber = Integer.parseInt(partNumberString);
    } catch (final NumberFormatException nfe) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "Part number must be an integer between 1 and 10000, inclusive");
    }
    if (partNumber < 1 || partNumber > 10000) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "Part number must be an integer between 1 and 10000, inclusive");
    }
  }

  private MediaType parseMediaType(final String contentType) {
    try {
      return MediaType.parseMediaType(contentType);
    } catch (final InvalidMediaTypeException e) {
      return FALLBACK_MEDIA_TYPE;
    }
  }

  private void validateMultipartParts(final String bucketName, final String filename,
      final String uploadId, final List<CompletedPart> requestedParts) throws S3Exception {
    validateMultipartParts(bucketName, filename, uploadId);

    final List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    final Map<Integer, String> uploadedPartsMap =
        uploadedParts
            .stream()
            .collect(Collectors.toMap(CompletedPart::getPartNumber, CompletedPart::getETag));

    Integer prevPartNumber = 0;
    for (final CompletedPart part : requestedParts) {
      if (!uploadedPartsMap.containsKey(part.getPartNumber())
          || !uploadedPartsMap.get(part.getPartNumber())
          .equals(part.getETag().replaceAll("^\"|\"$", ""))) {
        throw new S3Exception(BAD_REQUEST.value(), "InvalidPart",
            "One or more of the specified parts could not be found. The part might not have been "
                + "uploaded, or the specified entity tag might not have matched the part's entity"
                + " tag.");
      }
      if (part.getPartNumber() < prevPartNumber) {
        throw new S3Exception(BAD_REQUEST.value(), "InvalidPartOrder",
            "The list of parts was not in ascending order. The parts list must be specified in "
                + "order by part number.");
      }
      prevPartNumber = part.getPartNumber();
    }
  }

  private void validateMultipartParts(final String bucketName, final String filename,
      final String uploadId) throws S3Exception {
    verifyMultipartUploadExists(uploadId);
    final List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    if (uploadedParts.size() > 0) {
      for (int i = 0; i < uploadedParts.size() - 1; i++) {
        Part part = uploadedParts.get(i);
        if (part.getSize() < MINIMUM_PART_SIZE) {
          throw new S3Exception(BAD_REQUEST.value(), "EntityTooSmall",
              "Your proposed upload is smaller than the minimum allowed object size. "
                  + "Each part must be at least 5 MB in size, except the last part.");
        }
      }
    }
  }

  private void verifyMultipartUploadExists(final String uploadId) throws S3Exception {
    try {
      fileStore.getMultipartUpload(uploadId);
    } catch (IllegalArgumentException e) {
      throw new S3Exception(NOT_FOUND.value(), "NoSuchUpload",
          "The specified multipart upload does not exist. The upload ID might be invalid, or the "
              + "multipart upload might have been aborted or completed.");
    }
  }
}
