/*
 *  Copyright 2017-2021 Adobe.
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

import static com.adobe.testing.s3mock.MetadataDirective.METADATA_DIRECTIVE_COPY;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.METADATA_DIRECTIVE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID;
import static com.adobe.testing.s3mock.util.MetadataUtil.createUserMetadataHeaders;
import static com.adobe.testing.s3mock.util.MetadataUtil.getUserMetadata;
import static com.adobe.testing.s3mock.util.StringEncoding.decode;
import static com.adobe.testing.s3mock.util.StringEncoding.encode;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.NOT_MODIFIED;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.PRECONDITION_FAILED;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;

import com.adobe.testing.s3mock.domain.Bucket;
import com.adobe.testing.s3mock.domain.BucketContents;
import com.adobe.testing.s3mock.domain.FileStore;
import com.adobe.testing.s3mock.domain.S3Exception;
import com.adobe.testing.s3mock.domain.S3Object;
import com.adobe.testing.s3mock.domain.Tag;
import com.adobe.testing.s3mock.dto.BatchDeleteRequest;
import com.adobe.testing.s3mock.dto.BatchDeleteResponse;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadRequest;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.ObjectIdentifier;
import com.adobe.testing.s3mock.dto.ObjectRef;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.util.StringEncoding;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Controller to handle http requests.
 */
@RestController
class FileStoreController {

  private static final String ANY = "*";

  private static final String RANGES_BYTES = "bytes";

  private static final String STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD =
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

  private static final String RESPONSE_HEADER_CONTENT_TYPE = "response-content-type";
  private static final String RESPONSE_HEADER_CONTENT_LANGUAGE = "response-content-language";
  private static final String RESPONSE_HEADER_EXPIRES = "response-expires";
  private static final String RESPONSE_HEADER_CACHE_CONTROL = "response-cache-control";
  private static final String RESPONSE_HEADER_CONTENT_DISPOSITION = "response-content-disposition";
  private static final String RESPONSE_HEADER_CONTENT_ENCODING = "response-content-encoding";

  private static final String HEADER_X_AMZ_CONTENT_SHA256 = "x-amz-content-sha256";
  private static final String HEADER_X_AMZ_TAGGING = "x-amz-tagging";

  private static final Logger LOG = LoggerFactory.getLogger(FileStoreController.class);

  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");

  private static final Comparator<String> KEY_COMPARATOR = Comparator.naturalOrder();
  private static final Comparator<BucketContents> BUCKET_CONTENTS_COMPARATOR =
      Comparator.comparing(BucketContents::getKey, KEY_COMPARATOR);

  private static final MediaType FALLBACK_MEDIA_TYPE = new MediaType("binary", "octet-stream");

  @Autowired
  private FileStore fileStore;

  private final Map<String, String> fileStorePagingStateCache = new ConcurrentHashMap<>();

  /**
   * List all existing buckets.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html</p>
   *
   * @return List of all Buckets
   */
  @RequestMapping(value = "/", method = RequestMethod.GET, produces = {
      "application/xml"})
  public ResponseEntity<ListAllMyBucketsResult> listBuckets() {
    return ResponseEntity.ok(new ListAllMyBucketsResult(TEST_OWNER, fileStore.listBuckets()));
  }

  /**
   * Create a bucket.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html</p>
   *
   * @param bucketName name of the bucket that should be created.
   *
   * @return 200 OK if creation was successful.
   */
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.PUT)
  public ResponseEntity<String> createBucket(@PathVariable final String bucketName) {
    try {
      fileStore.createBucket(bucketName);
      return ResponseEntity.ok().build();
    } catch (final IOException e) {
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
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.HEAD)
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
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.DELETE)
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
   * Retrieves metadata from an object without returning the object itself.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html</p>
   *
   * @param bucketName name of the bucket to look in
   *
   * @return 200 with object metadata headers, 404 if not found.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      method = RequestMethod.HEAD)
  public ResponseEntity<String> headObject(@PathVariable final String bucketName,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);

    final S3Object s3Object = fileStore.getS3Object(bucketName, filename);
    if (s3Object != null) {
      return ResponseEntity.ok()
          .headers(headers -> headers.setAll(createUserMetadataHeaders(s3Object)))
          .headers(headers -> {
            if (s3Object.isEncrypted()) {
              headers.set(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, s3Object.getKmsKeyId());
            }
          })
          .contentType(parseMediaType(s3Object.getContentType()))
          .eTag("\"" + s3Object.getMd5() + "\"")
          .contentLength(Long.parseLong(s3Object.getSize()))
          .lastModified(s3Object.getLastModified())
          .build();
    } else {
      return ResponseEntity.status(NOT_FOUND).build();
    }
  }

  /**
   * Retrieve list of objects of a bucket.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html</p>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they starts with prefix
   * @param encodingtype whether to use URL encoding (encodingtype="url") or not
   *
   * @return {@link ListBucketResult} a list of objects in Bucket
   * @deprecated Long since replaced by ListObjectsV2, {@see #listObjectsInsideBucketV2}
   */
  @RequestMapping(
      value = "/{bucketName}",
      method = RequestMethod.GET,
      produces = {"application/xml"})
  @Deprecated
  public ResponseEntity<ListBucketResult> listObjectsInsideBucket(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(required = false) final String marker,
      @RequestParam(name = "encoding-type", required = false) final String encodingtype,
      @RequestParam(name = "max-keys", defaultValue = "1000",
          required = false) final Integer maxKeys) {
    verifyBucketExistence(bucketName);
    if (maxKeys < 0) {
      throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidRequest",
          "maxKeys should be non-negative");
    }
    if (!StringUtils.isEmpty(encodingtype) && !"url".equals(encodingtype)) {
      throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidRequest",
          "encodingtype can only be none or 'url'");
    }

    final boolean useUrlEncoding = Objects.equals("url", encodingtype);

    try {
      List<BucketContents> contents = getFilteredBucketContents(
          getBucketContents(bucketName, prefix), marker);

      boolean isTruncated = false;
      String nextMarker = null;

      final Set<String> commonPrefixes = new HashSet<>();
      if (null != delimiter) {
        collapseCommonPrefixes(prefix, delimiter, contents, commonPrefixes);
      }
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
              encodingtype, nextMarker, contents, returnCommonPrefixes));
    } catch (final IOException e) {
      LOG.error("Object(s) could not retrieved from bucket {}", bucketName, e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
  }

  private List<BucketContents> applyUrlEncoding(final List<BucketContents> contents) {
    return contents.stream().map(c -> new BucketContents(encode(c.getKey()),
        c.getLastModified(), c.getEtag(), c.getSize(), c.getStorageClass(), c.getOwner())).collect(
        Collectors.toList());
  }

  private Set<String> applyUrlEncoding(final Set<String> contents) {
    return contents.stream().map(StringEncoding::encode).collect(Collectors.toSet());
  }

  /**
   * Collapse all bucket elements with keys starting with some prefix up to the given delimiter into
   * one prefix entry. Collapsed elements are removed from the contents list.
   *
   * @param queryPrefix the key prefix as specified in the list request
   * @param delimiter the delimiter used to separate a prefix from the rest of the object name
   * @param contents the contents list
   * @param commonPrefixes the set of common prefixes
   *
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html">
   *     List Objects API Specification</a>
   */
  private void collapseCommonPrefixes(final String queryPrefix, final String delimiter,
      final List<BucketContents> contents, final Set<String> commonPrefixes) {
    final String normalizedQueryPrefix = queryPrefix == null ? "" : queryPrefix;

    for (final Iterator<BucketContents> i = contents.iterator(); i.hasNext(); ) {
      final BucketContents c = i.next();
      final String key = c.getKey();
      if (key.startsWith(normalizedQueryPrefix)) {
        final int delimiterIndex = key.indexOf(delimiter, normalizedQueryPrefix.length());
        if (delimiterIndex > 0) {
          commonPrefixes.add(key.substring(0, delimiterIndex + delimiter.length()));
          i.remove();
        }
      }
    }
  }

  /**
   * Retrieve list of objects of a bucket.
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html</p>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they starts with prefix
   * @param startAfter {@link String} return key names after a specific object key in your key
   *     space
   * @param maxKeys {@link Integer} set the maximum number of keys returned in the response
   *     body.
   * @param continuationToken {@link String} pagination token returned by previous request
   *
   * @return {@link ListBucketResultV2} a list of objects in Bucket
   */
  @RequestMapping(value = "/{bucketName}", params = "list-type=2",
      method = RequestMethod.GET,
      produces = {"application/xml"})
  public ResponseEntity<ListBucketResultV2> listObjectsInsideBucketV2(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(name = "encoding-type", required = false) final String encodingtype,
      @RequestParam(name = "start-after", required = false) final String startAfter,
      @RequestParam(name = "max-keys",
          defaultValue = "1000", required = false) final Integer maxKeys,
      @RequestParam(name = "continuation-token", required = false) final String continuationToken) {
    if (!StringUtils.isEmpty(encodingtype) && !"url".equals(encodingtype)) {
      throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidRequest",
          "encodingtype can only be none or 'url'");
    }

    final boolean useUrlEncoding = Objects.equals("url", encodingtype);

    verifyBucketExistence(bucketName);
    try {
      final List<BucketContents> contents = getBucketContents(bucketName, prefix);
      List<BucketContents> filteredContents;
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
        filteredContents = getFilteredBucketContents(contents, continueAfter);
        fileStorePagingStateCache.remove(continuationToken);
      } else {
        filteredContents = getFilteredBucketContents(contents, startAfter);
      }

      if (filteredContents.size() > maxKeys) {
        isTruncated = true;
        nextContinuationToken = UUID.randomUUID().toString();
        filteredContents = filteredContents.subList(0, maxKeys);
        fileStorePagingStateCache.put(nextContinuationToken,
            filteredContents.get(maxKeys - 1).getKey());
      }

      final Set<String> commonPrefixes = new HashSet<>();
      if (delimiter != null) {
        collapseCommonPrefixes(prefix, delimiter, filteredContents, commonPrefixes);
      }

      String returnPrefix = prefix;
      String returnStartAfter = startAfter;
      Set<String> returnCommonPrefixes = commonPrefixes;

      if (useUrlEncoding) {
        filteredContents = applyUrlEncoding(filteredContents);
        returnPrefix = isNotBlank(prefix) ? encode(prefix) : prefix;
        returnStartAfter = isNotBlank(startAfter) ? encode(startAfter) : startAfter;
        returnCommonPrefixes = applyUrlEncoding(commonPrefixes);
      }

      return ResponseEntity.ok(new ListBucketResultV2(bucketName, returnPrefix, maxKeys,
          isTruncated, filteredContents, returnCommonPrefixes,
          continuationToken, String.valueOf(filteredContents.size()),
          nextContinuationToken, returnStartAfter, encodingtype));
    } catch (final IOException e) {
      LOG.error("Object(s) could not retrieved from bucket {}", bucketName, e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
  }

  private List<BucketContents> getFilteredBucketContents(final List<BucketContents> contents,
      final String startAfter) {
    if (startAfter != null && !"".equals(startAfter)) {
      return contents
          .stream()
          .filter(p -> KEY_COMPARATOR.compare(p.getKey(), startAfter) > 0)
          .collect(Collectors.toList());
    } else {
      return contents;
    }
  }

  private List<BucketContents> getBucketContents(final String bucketName,
      final String prefix) throws IOException {
    final String encodedPrefix = null != prefix ? encode(prefix) : null;

    final List<S3Object> s3Objects = fileStore.getS3Objects(bucketName, encodedPrefix);

    LOG.debug(String.format("Found %s objects in bucket %s", s3Objects.size(), bucketName));
    return s3Objects.stream().map(s3Object -> new BucketContents(
        decode(s3Object.getName()),
        s3Object.getModificationDate(), s3Object.getMd5(),
        s3Object.getSize(), "STANDARD", TEST_OWNER))
        // List Objects results are expected to be sorted by key
        .sorted(BUCKET_CONTENTS_COMPARATOR)
        .collect(Collectors.toList());
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
  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.PUT)
  public ResponseEntity<String> putObject(@PathVariable final String bucketName,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION, required = false) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, required = false)
      final String kmsKeyId,
      @RequestHeader(name = HEADER_X_AMZ_TAGGING, required = false) final List<Tag> tags,
      @RequestHeader(value = CONTENT_ENCODING, required = false) String contentEncoding,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader(value = HEADER_X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    final S3Object s3Object;
    try (final ServletInputStream inputStream = request.getInputStream()) {
      final Map<String, String> userMetadata = getUserMetadata(request);
      s3Object =
          fileStore.putS3Object(bucketName,
              filename,
              parseMediaType(contentType).toString(),
              contentEncoding,
              inputStream,
              isV4ChunkedWithSigningEnabled(sha256Header),
              userMetadata,
              encryption,
              kmsKeyId);

      fileStore.setObjectTags(bucketName, filename, tags);

      return ResponseEntity
          .ok()
          .eTag("\"" + s3Object.getMd5() + "\"")
          .lastModified(s3Object.getLastModified())
          .header(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, kmsKeyId)
          .build();
    }
  }

  private boolean isV4ChunkedWithSigningEnabled(final String sha256Header) {
    return sha256Header != null && sha256Header.equals(STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD);
  }

  /**
   * Copies an object to another bucket.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html</p>
   *
   * @param destinationBucket name of the destination bucket
   * @param objectRef path to source object
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   *
   * @return {@link CopyObjectResult}
   *
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{destinationBucket:.+}/**",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE
      },
      produces = "application/xml; charset=utf-8")
  public ResponseEntity<CopyObjectResult> copyObject(@PathVariable final String destinationBucket,
      @RequestHeader(value = COPY_SOURCE) final ObjectRef objectRef,
      @RequestHeader(value = METADATA_DIRECTIVE,
          defaultValue = METADATA_DIRECTIVE_COPY) final MetadataDirective metadataDirective,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION, required = false) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(destinationBucket);
    final String destinationFile = filenameFrom(destinationBucket, request);

    final CopyObjectResult copyObjectResult;
    if (MetadataDirective.REPLACE == metadataDirective) {
      copyObjectResult = fileStore.copyS3ObjectEncrypted(objectRef.getBucket(),
          encode(objectRef.getKey()),
          destinationBucket,
          destinationFile,
          encryption,
          kmsKeyId,
          getUserMetadata(request));
    } else {
      copyObjectResult = fileStore.copyS3ObjectEncrypted(objectRef.getBucket(),
          encode(objectRef.getKey()),
          destinationBucket,
          destinationFile,
          encryption,
          kmsKeyId);
    }

    if (copyObjectResult == null) {
      return ResponseEntity.notFound().header(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, kmsKeyId)
          .build();
    }
    return ResponseEntity.ok().header(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, kmsKeyId)
        .body(copyObjectResult);
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
      value = "/{bucketName:.+}/**",
      method = RequestMethod.GET,
      produces = "application/xml")
  public ResponseEntity<StreamingResponseBody> getObject(@PathVariable final String bucketName,
      @RequestHeader(value = RANGE, required = false) final Range range,
      @RequestHeader(value = IF_MATCH, required = false) final List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) final List<String> noMatch,
      final HttpServletRequest request) throws IOException {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final S3Object s3Object = verifyObjectExistence(bucketName, filename);

    verifyObjectMatching(match, noMatch, s3Object.getMd5());

    if (range != null) {
      return getObjectWithRange(range, s3Object);
    }

    return ResponseEntity
        .ok()
        .eTag("\"" + s3Object.getMd5() + "\"")
        .header(HttpHeaders.CONTENT_ENCODING, s3Object.getContentEncoding())
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .header(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ANY)
        .headers(headers -> headers.setAll(createUserMetadataHeaders(s3Object)))
        .lastModified(s3Object.getLastModified())
        .contentLength(s3Object.getDataFile().length())
        .contentType(parseMediaType(s3Object.getContentType()))
        .headers(headers -> headers.setAll(addOverrideHeaders(request.getQueryString())))
        .body(outputStream -> Files.copy(s3Object.getDataFile().toPath(), outputStream));
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

  /**
   * The DELETE operation removes an object.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html</p>
   *
   * @param bucketName name of bucket containing the object.
   *
   * @return ResponseEntity with Status Code 204 if object was successfully deleted.
   */
  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.DELETE)
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
      params = "delete",
      method = RequestMethod.POST,
      produces = {"application/xml"})
  public ResponseEntity<BatchDeleteResponse> batchDeleteObjects(
      @PathVariable final String bucketName,
      @RequestBody final BatchDeleteRequest body) {
    verifyBucketExistence(bucketName);
    final BatchDeleteResponse response = new BatchDeleteResponse();
    for (final ObjectIdentifier object : body.getObjectsToDelete()) {
      try {
        if (fileStore.deleteObject(bucketName, encode(object.getKey()))) {
          response.addDeletedObject(object);
        }
      } catch (final IOException e) {
        LOG.error("Object could not be deleted!", e);
      }
    }

    return ResponseEntity.ok(response);
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
      params = "tagging",
      method = RequestMethod.GET)
  public ResponseEntity<Tagging> getObjectTagging(@PathVariable final String bucketName,
      final HttpServletRequest request) {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final S3Object s3Object = verifyObjectExistence(bucketName, filename);

    final List<Tag> tagList = new ArrayList<>(s3Object.getTags());
    final Tagging result = new Tagging(tagList);

    return ResponseEntity
        .ok()
        .eTag("\"" + s3Object.getMd5() + "\"")
        .lastModified(s3Object.getLastModified())
        .body(result);
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
      params = "tagging",
      method = RequestMethod.PUT)
  public ResponseEntity<String> putObjectTagging(@PathVariable final String bucketName,
      @RequestBody final Tagging body,
      final HttpServletRequest request) {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final S3Object s3Object = verifyObjectExistence(bucketName, filename);

    try {
      fileStore.setObjectTags(bucketName, filename, body.getTagSet());
      return ResponseEntity
          .ok()
          .eTag("\"" + s3Object.getMd5() + "\"")
          .lastModified(s3Object.getLastModified())
          .build();
    } catch (final IOException e) {
      LOG.error("Tags could not be set!", e);
      return ResponseEntity.status(INTERNAL_SERVER_ERROR).build();
    }
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
      params = "uploads",
      method = RequestMethod.POST,
      produces = "application/xml")
  public ResponseEntity<InitiateMultipartUploadResult> initiateMultipartUpload(
      @PathVariable final String bucketName,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION, required = false) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, required = false)
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
      value = "/{bucketName:.+}/",
      params = {"uploads"},
      method = RequestMethod.GET,
      produces = "application/xml")
  public ResponseEntity<ListMultipartUploadsResult> listMultipartUploads(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam final String uploads) {
    verifyBucketExistence(bucketName);

    final List<MultipartUpload> multipartUploads =
        fileStore.listMultipartUploads().stream()
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
   * Aborts a multipart upload for a given uploadId.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {"uploadId"},
      method = RequestMethod.DELETE,
      produces = "application/xml")
  public ResponseEntity<Void> abortMultipartUpload(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    fileStore.abortMultipartUpload(bucketName, filename, uploadId);
    return ResponseEntity.noContent().build();
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
      params = {"uploadId"},
      method = RequestMethod.GET,
      produces = "application/xml")
  public ResponseEntity<ListPartsResult> multipartListParts(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);

    final List<Part> parts = fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    return ResponseEntity.ok(new ListPartsResult(bucketName, filename, uploadId, parts));
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
      params = {"uploadId", "partNumber"},
      headers = {
          NOT_COPY_SOURCE,
          NOT_COPY_SOURCE_RANGE
      },
      method = RequestMethod.PUT)
  public ResponseEntity<Void> putObjectPart(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION, required = false) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, required = false)
      final String kmsKeyId,
      @RequestHeader(value = HEADER_X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
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
      value = "/{destinationBucket:.+}/**",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          COPY_SOURCE_RANGE
      })
  public ResponseEntity<CopyPartResult> copyObjectPart(
      @RequestHeader(value = COPY_SOURCE) final ObjectRef copySource,
      @RequestHeader(value = COPY_SOURCE_RANGE) final Range copyRange,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION, required = false) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      @PathVariable final String destinationBucket,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(destinationBucket);

    final String destinationFile = filenameFrom(destinationBucket, request);
    final String partEtag = fileStore.copyPart(copySource.getBucket(),
        copySource.getKey(),
        (int) copyRange.getStart(),
        (int) copyRange.getEnd(),
        partNumber,
        destinationBucket,
        destinationFile,
        uploadId
    );

    return ResponseEntity.ok(CopyPartResult.from(new Date(), "\"" + partEtag + "\""));
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
      params = {"uploadId"},
      method = RequestMethod.POST,
      produces = "application/xml")
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION, required = false) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, required = false)
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
   * @param s3Object {@link S3Object}
   */
  private ResponseEntity<StreamingResponseBody> getObjectWithRange(final Range range,
      final S3Object s3Object) {
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
        .eTag("\"" + s3Object.getMd5() + "\"")
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

  private S3Object verifyObjectExistence(final String bucketName, final String filename) {
    final S3Object s3Object = fileStore.getS3Object(bucketName, filename);
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
      throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidRequest",
          "Part number must be an integer between 1 and 10000, inclusive");
    }
    if (partNumber < 1 || partNumber > 10000) {
      throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidRequest",
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
      final String uploadId, final List<Part> requestedParts) {
    final List<Part> uploadedParts =
        fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    final Map<Integer, String> uploadedPartsMap =
        uploadedParts.stream().collect(Collectors.toMap(Part::getPartNumber, Part::getETag));

    Integer prevPartNumber = 0;
    for (final Part part : requestedParts) {
      if (!uploadedPartsMap.containsKey(part.getPartNumber())
          || !uploadedPartsMap.get(part.getPartNumber())
          .equals(part.getETag().replaceAll("^\"|\"$", ""))) {
        throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidPart",
            "One or more of the specified parts could not be found. The part might not have been "
                + "uploaded, or the specified entity tag might not have matched the part's entity"
                + " tag.");
      }
      if (part.getPartNumber() < prevPartNumber) {
        throw new S3Exception(HttpStatus.BAD_REQUEST.value(), "InvalidPartOrder",
            "The list of parts was not in ascending order. The parts list must be specified in "
                + "order by part number.");
      }
      prevPartNumber = part.getPartNumber();
    }
  }
}
