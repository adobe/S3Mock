/*
 *  Copyright 2017-2019 Adobe.
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

import static com.adobe.testing.s3mock.util.AwsHttpHeaders.COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.METADATA_DIRECTIVE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_COPY_SOURCE_RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID;
import static com.adobe.testing.s3mock.util.MetadataUtil.addUserMetadata;
import static com.adobe.testing.s3mock.util.MetadataUtil.getUserMetadata;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.NOT_MODIFIED;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;
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
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.DeletedObject;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListAllMyBucketsResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.ObjectRef;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.Tagging;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.util.AbstractMap.SimpleImmutableEntry;
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
import java.util.stream.Collectors;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.eclipse.jetty.util.TypeUtil;
import org.eclipse.jetty.util.UrlEncoded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
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
  private static final String ABSENT_ENCRYPTION = null;
  private static final String ABSENT_KEY_ID = null;
  private static final String METADATA_DIRECTIVE_COPY = "COPY";
  private static final String METADATA_DIRECTIVE_REPLACE = "REPLACE";

  private static final Logger LOG = LoggerFactory.getLogger(FileStoreController.class);

  private static final Owner TEST_OWNER = new Owner(123, "s3-mock-file-store");

  private static final Comparator<String> KEY_COMPARATOR = Comparator.naturalOrder();
  private static final Comparator<BucketContents> BUCKET_CONTENTS_COMPARATOR =
      Comparator.comparing(BucketContents::getKey, KEY_COMPARATOR);

  @Autowired
  private FileStore fileStore;

  @Autowired
  private Cache fileStorePagingStateCache;

  /**
   * Lists all existing buckets.
   *
   * @return a list of all Buckets
   */
  @RequestMapping(value = "/", method = RequestMethod.GET, produces = {
      "application/x-www-form-urlencoded"})
  @ResponseBody
  public ListAllMyBucketsResult listBuckets() {
    return new ListAllMyBucketsResult(TEST_OWNER, fileStore.listBuckets());
  }

  /**
   * Creates a bucket.
   *
   * @param bucketName name of the bucket that should be created.
   *
   * @return ResponseEntity with Status Code
   */
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.PUT)
  public ResponseEntity<String> putBucket(@PathVariable final String bucketName) {
    try {
      fileStore.createBucket(bucketName);
      return new ResponseEntity<>(OK);
    } catch (final IOException e) {
      LOG.error("Bucket could not be created!", e);
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Operation to determine if a bucket exists.
   *
   * @param bucketName name of the Bucket to be checked.
   *
   * @return ResponseEntity Code 200 OK; 404 Not found.
   */
  @RequestMapping(value = "/{bucketName}", method = RequestMethod.HEAD)
  public ResponseEntity<String> headBucket(@PathVariable final String bucketName) {
    if (fileStore.doesBucketExist(bucketName)) {
      return new ResponseEntity<>(OK);
    } else {
      return new ResponseEntity<>(NOT_FOUND);
    }
  }

  /**
   * Deletes a specified bucket.
   *
   * @param bucketName name of bucket containing the object.
   *
   * @return ResponseEntity with Status Code 204 if object was successfully deleted; 404 if not
   *     found
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
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }

    if (deleted) {
      return new ResponseEntity<>(NO_CONTENT);
    } else {
      return new ResponseEntity<>(NOT_FOUND);
    }
  }

  /**
   * Retrieves metadata from an object without returning the object itself.
   *
   * @param bucketName name of the bucket to look in
   *
   * @return ResponseEntity containing metadata and status
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

      addUserMetadata(responseHeaders::add, s3Object);

      return new ResponseEntity<>(responseHeaders, OK);
    } else {
      return new ResponseEntity<>(NOT_FOUND);
    }
  }

  /**
   * Retrieve list of objects of a bucket see http://docs.aws.amazon
   * .com/AmazonS3/latest/API/RESTBucketGET.html
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they starts with prefix
   * @param encodingtype whether to use URL encoding (encodingtype="url") or not
   * @param response {@link HttpServletResponse}
   *
   * @return {@link ListBucketResult} a list of objects in Bucket
   *
   * @throws IOException IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{bucketName}",
      method = RequestMethod.GET,
      produces = {"application/x-www-form-urlencoded"})
  @ResponseBody
  public ListBucketResult listObjectsInsideBucket(@PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(required = false) final String marker,
      @RequestParam(name = "encoding-type", required = false) final String encodingtype,
      @RequestParam(name = "max-keys", defaultValue = "1000",
          required = false) final Integer maxKeys,
      final HttpServletResponse response) throws IOException {
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

      if (useUrlEncoding) {
        contents = applyUrlEncoding(contents);
      }

      return new ListBucketResult(bucketName, prefix, marker, maxKeys, isTruncated, nextMarker,
          contents, commonPrefixes);
    } catch (final IOException e) {
      LOG.error(String.format("Object(s) could not retrieved from bucket %s", bucketName));
      response.sendError(500, e.getMessage());
    }

    return null;
  }

  private List<BucketContents> applyUrlEncoding(final List<BucketContents> contents) {
    return contents.stream().map(c -> new BucketContents(UrlEncoded.encodeString(c.getKey()),
        c.getLastModified(), c.getEtag(), c.getSize(), c.getStorageClass(), c.getOwner())).collect(
        Collectors.toList());
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
   * Retrieve list of objects of a bucket see https://docs.aws.amazon
   * .com/AmazonS3/latest/API/v2-RESTBucketGET.html
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they starts with prefix
   * @param startAfter {@link String} return key names after a specific object key in your key
   *     space
   * @param maxKeysParam {@link String} set the maximum number of keys returned in the response
   *     body.
   * @param continuationToken {@link String} pagination token returned by previous request
   * @param response {@link HttpServletResponse}
   *
   * @return {@link ListBucketResult} a list of objects in Bucket
   *
   * @throws IOException IOException If an input or output exception occurs
   */
  @RequestMapping(value = "/{bucketName}", params = "list-type=2",
      method = RequestMethod.GET,
      produces = {"application/x-www-form-urlencoded"})
  @ResponseBody
  public ListBucketResultV2 listObjectsInsideBucketV2(@PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(name = "encoding-type", required = false) final String encodingtype,
      @RequestParam(name = "start-after", required = false) final String startAfter,
      @RequestParam(name = "max-keys",
          defaultValue = "1000", required = false) final String maxKeysParam,
      @RequestParam(name = "continuation-token", required = false) final String continuationToken,
      final HttpServletResponse response) throws IOException {
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
        final String continueAfter =
            fileStorePagingStateCache.get(continuationToken).get().toString();
        filteredContents = getFilteredBucketContents(contents, continueAfter);
        fileStorePagingStateCache.evict(continuationToken);
      } else {
        filteredContents = getFilteredBucketContents(contents, startAfter);
      }

      final int maxKeys = Integer.parseInt(maxKeysParam);
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

      if (useUrlEncoding) {
        filteredContents = applyUrlEncoding(filteredContents);
      }

      return new ListBucketResultV2(bucketName, prefix, maxKeysParam,
          isTruncated, filteredContents, commonPrefixes,
          continuationToken, String.valueOf(filteredContents.size()),
          nextContinuationToken, startAfter);
    } catch (final IOException e) {
      LOG.error(String.format("Object(s) could not retrieved from bucket %s", bucketName));
      response.sendError(500, e.getMessage());
    }

    return null;
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
    final String encodedPrefix = null != prefix ? objectNameToFileName(prefix) : null;

    final List<S3Object> s3Objects = fileStore.getS3Objects(bucketName, encodedPrefix);

    LOG.debug(String.format("Found %s objects in bucket %s", s3Objects.size(), bucketName));
    return s3Objects.stream().map(s3Object -> new BucketContents(
        fileNameToObjectName(s3Object.getName()),
        s3Object.getModificationDate(), s3Object.getMd5(),
        s3Object.getSize(), "STANDARD", TEST_OWNER))
        // List Objects results are expected to be sorted by key
        .sorted(BUCKET_CONTENTS_COMPARATOR)
        .collect(Collectors.toList());
  }

  /**
   * Adds an object to a bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param request http servlet request
   *
   * @return ResponseEntity with Status Code and ETag
   */
  @RequestMapping(value = "/{bucketName:.+}/**", method = RequestMethod.PUT)
  public ResponseEntity<String> putObject(@PathVariable final String bucketName,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    try (final ServletInputStream inputStream = request.getInputStream()) {
      final Map<String, String> userMetadata = getUserMetadata(request);
      final S3Object s3Object = fileStore.putS3Object(bucketName,
          filename,
          request.getContentType(),
          request.getHeader(HttpHeaders.CONTENT_ENCODING),
          inputStream,
          isV4ChunkedWithSigningEnabled(request),
          userMetadata);

      addTagsFromReq(request, bucketName, filename);

      final HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
      responseHeaders.setLastModified(s3Object.getLastModified());
      addUserMetadata(responseHeaders::add, s3Object);
      return new ResponseEntity<>(responseHeaders, OK);
    } catch (final IOException e) {
      LOG.error("Object could not be saved!", e);
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }
  }

  private void addTagsFromReq(final HttpServletRequest request,
      final String bucketName,
      final String filename) throws IOException {
    final String header = request.getHeader(HEADER_X_AMZ_TAGGING);
    if (header != null && !header.isEmpty()) {
      final List<Tag> tags = new ArrayList<>();
      new UrlEncoded(header)
          .forEach((tag, values) -> tags.add(new Tag(tag, values.get(0))));

      fileStore.setObjectTags(bucketName, filename, tags);
    }
  }

  private boolean isV4ChunkedWithSigningEnabled(final HttpServletRequest request) {
    final String sha256Header = request.getHeader(HEADER_X_AMZ_CONTENT_SHA256);
    return sha256Header != null && sha256Header.equals(STREAMING_AWS_4_HMAC_SHA_256_PAYLOAD);
  }

  /**
   * Adds an encrypted object to a bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html</p>
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
      value = "/{bucketName:.+}/**",
      headers = {
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          NOT_COPY_SOURCE
      },
      method = RequestMethod.PUT)
  public ResponseEntity<String> putObjectEncrypted(@PathVariable final String bucketName,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    final S3Object s3Object;
    try (final ServletInputStream inputStream = request.getInputStream()) {
      final Map<String, String> userMetadata = getUserMetadata(request);
      s3Object =
          fileStore.putS3ObjectWithKMSEncryption(bucketName,
              filename,
              request.getContentType(),
              inputStream,
              isV4ChunkedWithSigningEnabled(request),
              userMetadata,
              encryption,
              kmsKeyId);

      addTagsFromReq(request, bucketName, filename);

      final HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
      responseHeaders.setLastModified(s3Object.getLastModified());
      responseHeaders.add(SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID, kmsKeyId);

      return new ResponseEntity<>(responseHeaders, OK);
    }
  }

  /**
   * Copies an object to another bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html</p>
   *
   * @param destinationBucket name of the destination bucket
   * @param objectRef path to source object
   * @param response response object
   *
   * @return {@link CopyObjectResult}
   *
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{destinationBucket:.+}/**",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          NOT_SERVER_SIDE_ENCRYPTION
      },
      produces = "application/x-www-form-urlencoded; charset=utf-8")
  @ResponseBody
  public CopyObjectResult copyObject(@PathVariable final String destinationBucket,
      @RequestHeader(value = COPY_SOURCE) final ObjectRef objectRef,
      @RequestHeader(value = METADATA_DIRECTIVE,
          defaultValue = METADATA_DIRECTIVE_COPY) final String metadataDirective,
      final HttpServletRequest request,
      final HttpServletResponse response) throws IOException {

    return copyObject(destinationBucket,
        objectRef,
        metadataDirective,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        request,
        response);
  }

  /**
   * Copies an object encrypted to another bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html</p>
   *
   * @param destinationBucket name of the destination bucket
   * @param objectRef path to source object
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   * @param response response object
   *
   * @return {@link CopyObjectResult}
   *
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{destinationBucket:.+}/**",
      method = RequestMethod.PUT,
      headers = {
          COPY_SOURCE,
          SERVER_SIDE_ENCRYPTION
      },
      produces = "application/x-www-form-urlencoded; charset=utf-8")
  @ResponseBody
  public CopyObjectResult copyObject(@PathVariable final String destinationBucket,
      @RequestHeader(value = COPY_SOURCE) final ObjectRef objectRef,
      @RequestHeader(value = METADATA_DIRECTIVE,
          defaultValue = METADATA_DIRECTIVE_COPY) final String metadataDirective,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      final HttpServletRequest request,
      final HttpServletResponse response) throws IOException {
    verifyBucketExistence(destinationBucket);
    validateMetadataDirective(metadataDirective);
    final String destinationFile = filenameFrom(destinationBucket, request);

    final CopyObjectResult copyObjectResult;
    if (METADATA_DIRECTIVE_REPLACE.equals(metadataDirective)) {
      copyObjectResult = fileStore.copyS3ObjectEncrypted(objectRef.getBucket(),
          objectNameToFileName(objectRef.getKey()),
          destinationBucket,
          destinationFile,
          encryption,
          kmsKeyId,
          getUserMetadata(request));
    } else {
      copyObjectResult = fileStore.copyS3ObjectEncrypted(objectRef.getBucket(),
          objectNameToFileName(objectRef.getKey()),
          destinationBucket,
          destinationFile,
          encryption,
          kmsKeyId);
    }

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
   * Returns the File identified by bucketName and fileName.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html</p>
   *
   * @param bucketName The Buckets names
   * @param range byte range
   * @param response response object
   *
   * @throws IOException If an input or output exception occurs
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      method = RequestMethod.GET,
      produces = "application/x-www-form-urlencoded")
  public void getObject(@PathVariable final String bucketName,
      @RequestHeader(value = RANGE, required = false) final Range range,
      @RequestHeader(value = IF_MATCH, required = false) final List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) final List<String> noMatch,
      final HttpServletRequest request,
      final HttpServletResponse response) throws IOException {
    final String filename = filenameFrom(bucketName, request);

    verifyBucketExistence(bucketName);

    final S3Object s3Object = verifyObjectExistence(bucketName, filename);

    verifyObjectMatching(match, noMatch, s3Object.getMd5());

    if (range != null) {
      getObjectWithRange(response, range, s3Object);
    } else {
      response.setHeader(HttpHeaders.ETAG, "\"" + s3Object.getMd5() + "\"");
      response.setContentType(s3Object.getContentType());
      response.setHeader(HttpHeaders.CONTENT_ENCODING, s3Object.getContentEncoding());
      response.setContentLengthLong(s3Object.getDataFile().length());
      response.setHeader(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES);
      response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ANY);
      response.setDateHeader(HttpHeaders.LAST_MODIFIED, s3Object.getLastModified());
      addUserMetadata(response::addHeader, s3Object);
      addOverrideHeaders(response, request.getQueryString());

      try (final OutputStream outputStream = response.getOutputStream()) {
        Files.copy(s3Object.getDataFile().toPath(), outputStream);
      }
    }

  }

  private void addOverrideHeaders(final HttpServletResponse response, final String query) {
    if (isNotBlank(query)) {
      Arrays.stream(query.split("&"))
          .map(this::splitQueryParameter)
          .forEach((h) -> addOverrideHeader(response, h.getKey(), h.getValue()));
    }
  }

  private SimpleImmutableEntry<String, String> splitQueryParameter(final String param) {
    try {
      final String key = URLDecoder.decode(substringBefore(param, "="), UTF_8.name());
      final String value = URLDecoder.decode(substringAfter(param, "="), UTF_8.name());
      return new SimpleImmutableEntry<>(key, value);
    } catch (final UnsupportedEncodingException e) {
      throw new AssertionError(UTF_8.name() + " is unknown");
    }
  }

  private void addOverrideHeader(final HttpServletResponse response, final String name,
      final String value) {
    switch (name) {
      case RESPONSE_HEADER_CACHE_CONTROL:
        response.setHeader(HttpHeaders.CACHE_CONTROL, value);
        break;
      case RESPONSE_HEADER_CONTENT_DISPOSITION:
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, value);
        break;
      case RESPONSE_HEADER_CONTENT_ENCODING:
        response.setHeader(HttpHeaders.CONTENT_ENCODING, value);
        break;
      case RESPONSE_HEADER_CONTENT_LANGUAGE:
        response.setHeader(HttpHeaders.CONTENT_LANGUAGE, value);
        break;
      case RESPONSE_HEADER_CONTENT_TYPE:
        response.setContentType(value);
        break;
      case RESPONSE_HEADER_EXPIRES:
        response.setHeader(HttpHeaders.EXPIRES, value);
        break;
      default:
        // Only the above header overrides are supported by S3
    }
  }

  /**
   * The DELETE operation removes an object.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html</p>
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
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }

    return new ResponseEntity<>(NO_CONTENT);
  }

  /**
   * The batch DELETE operation removes multiple objects.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html</p>
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
      produces = {"application/x-www-form-urlencoded"})
  public BatchDeleteResponse batchDeleteObjects(@PathVariable final String bucketName,
      @RequestBody final BatchDeleteRequest body) {
    verifyBucketExistence(bucketName);
    final BatchDeleteResponse response = new BatchDeleteResponse();
    for (final BatchDeleteRequest.ObjectToDelete object : body.getObjectsToDelete()) {
      try {
        if (fileStore.deleteObject(bucketName, objectNameToFileName(object.getKey()))) {
          final DeletedObject deletedObject = new DeletedObject();
          deletedObject.setKey(object.getKey());
          response.addDeletedObject(deletedObject);
        }
      } catch (final IOException e) {
        LOG.error("Object could not be deleted!", e);
      }
    }

    return response;
  }

  /**
   * Returns the tags identified by bucketName and fileName.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtagging.html</p>
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

    final HttpHeaders responseHeaders = new HttpHeaders();
    responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
    responseHeaders.setLastModified(s3Object.getLastModified());

    return ResponseEntity.ok().headers(responseHeaders).body(result);
  }

  /**
   * Sets tags for a file identified by bucketName and fileName.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUTtagging.html</p>
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
      final HttpHeaders responseHeaders = new HttpHeaders();
      responseHeaders.setETag("\"" + s3Object.getMd5() + "\"");
      responseHeaders.setLastModified(s3Object.getLastModified());

      return new ResponseEntity<>(responseHeaders, OK);
    } catch (final IOException e) {
      LOG.error("Tags could not be set!", e);
      return new ResponseEntity<>(e.getMessage(), INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Initiates a multipart upload.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = "uploads",
      method = RequestMethod.POST,
      produces = "application/x-www-form-urlencoded")
  public InitiateMultipartUploadResult initiateMultipartUpload(
      @PathVariable final String bucketName,
      final HttpServletRequest request) {

    return initiateMultipartUpload(bucketName,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        request);
  }

  /**
   * Initiates a multipart upload accepting encryption headers.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = "uploads",
      headers = {
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID
      },
      method = RequestMethod.POST,
      produces = "application/x-www-form-urlencoded")
  public InitiateMultipartUploadResult initiateMultipartUpload(
      @PathVariable final String bucketName,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID) final String kmsKeyId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    final Map<String, String> userMetadata = getUserMetadata(request);

    final String uploadId = UUID.randomUUID().toString();
    fileStore.prepareMultipartUpload(bucketName, filename, request.getContentType(),
        request.getHeader(HttpHeaders.CONTENT_ENCODING), uploadId,
        TEST_OWNER, TEST_OWNER, userMetadata);

    return new InitiateMultipartUploadResult(bucketName, fileNameToObjectName(filename), uploadId);
  }

  /**
   * Lists all in-progress multipart uploads.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListMPUpload.html</p>
   *
   * <p>Not yet supported request parameters: delimiter, encoding-type, max-uploads, key-marker,
   * prefix, upload-id-marker.</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link ListMultipartUploadsResult}
   */
  @RequestMapping(
      value = "/{bucketName:.+}/",
      params = {"uploads"},
      method = RequestMethod.GET,
      produces = "application/x-www-form-urlencoded")
  public ListMultipartUploadsResult listMultipartUploads(@PathVariable final String bucketName,
      @RequestParam final String /*unused */ uploads) {
    verifyBucketExistence(bucketName);

    final List<MultipartUpload> multipartUploads =
        fileStore.listMultipartUploads().stream().map(m ->
            new MultipartUpload(fileNameToObjectName(m.getKey()), m.getUploadId(), m.getOwner(),
                m.getInitiator(), m.getInitiated()))
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
    final String prefix = null;
    final List<String> commmonPrefixes = Collections.emptyList();

    return new ListMultipartUploadsResult(bucketName, keyMarker, delimiter, prefix, uploadIdMarker,
        maxUploads, isTruncated, nextKeyMarker, nextUploadIdMarker, multipartUploads,
        commmonPrefixes);
  }

  /**
   * Aborts a multipart upload for a given uploadId.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      params = {"uploadId"},
      method = RequestMethod.DELETE,
      produces = "application/x-www-form-urlencoded")
  public void abortMultipartUpload(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);
    fileStore.abortMultipartUpload(bucketName, filename, uploadId);
  }

  /**
   * Lists all parts a file multipart upload.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html</p>
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
      produces = "application/x-www-form-urlencoded")
  public ListPartsResult multipartListParts(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);

    final List<Part> parts = fileStore.getMultipartUploadParts(bucketName, filename, uploadId);
    return new ListPartsResult(bucketName, filename, uploadId, parts);
  }

  /**
   * Adds an object to a bucket accepting encryption headers.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html</p>
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
          NOT_COPY_SOURCE_RANGE,
          SERVER_SIDE_ENCRYPTION
      },
      method = RequestMethod.PUT)
  public ResponseEntity<CopyPartResult> putObjectPart(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(
          value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID,
          required = false) final String kmsKeyId,
      final HttpServletRequest request) throws IOException {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);

    final String etag = fileStore.putPart(bucketName,
        filename,
        uploadId,
        partNumber,
        request.getInputStream(),
        isV4ChunkedWithSigningEnabled(request));

    final HttpHeaders responseHeaders = new HttpHeaders();
    final String quotedEtag = "\"" + etag + "\"";
    responseHeaders.setETag(quotedEtag);

    return new ResponseEntity<>(responseHeaders, OK);
  }

  /**
   * Adds an object to a bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param partNumber number of the part to upload
   * @param request {@link HttpServletRequest} of this request
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
  public ResponseEntity<CopyPartResult> putObjectPart(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {

    return putObjectPart(bucketName,
        uploadId,
        partNumber,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        request);
  }

  /**
   * Uploads a part by copying data from an existing object as data source.
   *
   * <p>https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html</p>
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

  @RequestMapping(
      value = "/{destinationBucket:.+}/**",
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
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      final HttpServletRequest request) throws IOException {
    return copyObjectPart(copySource,
        copyRange,
        ABSENT_ENCRYPTION,
        ABSENT_KEY_ID,
        destinationBucket,
        uploadId,
        partNumber,
        request);
  }

  /**
   * Adds an object to a bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html</p>
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
      method = RequestMethod.POST)
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUpload(
      @PathVariable final String bucketName,
      @RequestParam final String uploadId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);

    final String filename = filenameFrom(bucketName, request);

    final String eTag =
        fileStore.completeMultipartUpload(bucketName, filename, uploadId);

    return new ResponseEntity<>(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName,
            filename, eTag), new HttpHeaders(), OK);
  }

  /**
   * Adds an object to a bucket.
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html</p>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   * @param request {@link HttpServletRequest} of this request.
   *
   * @return {@link CompleteMultipartUploadResult}
   */
  @RequestMapping(
      value = "/{bucketName:.+}/**",
      headers = {
          SERVER_SIDE_ENCRYPTION,
          SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID
      },
      params = {"uploadId"},
      method = RequestMethod.POST)
  public ResponseEntity<CompleteMultipartUploadResult> completeMultipartUploadEncrypted(
      @PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION) final String encryption,
      @RequestHeader(value = SERVER_SIDE_ENCRYPTION_AWS_KMS_KEYID) final String kmsKeyId,
      final HttpServletRequest request) {
    verifyBucketExistence(bucketName);
    final String filename = filenameFrom(bucketName, request);

    final String eTag = fileStore.completeMultipartUpload(bucketName,
        filename,
        uploadId,
        encryption,
        kmsKeyId);

    return new ResponseEntity<>(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName,
            filename, eTag), new HttpHeaders(), OK);
  }

  /**
   * supports range different range ends. eg. if content has 100 bytes, the range request could be:
   * bytes=10-100, 10--1 and 10-200
   *
   * <p>http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html</p>
   *
   * @param response {@link HttpServletResponse}
   * @param range {@link String}
   * @param s3Object {@link S3Object}
   *
   * @throws IOException if invalid range request value
   */
  private void getObjectWithRange(final HttpServletResponse response, final Range range,
      final S3Object s3Object)
      throws IOException {
    final long fileSize = s3Object.getDataFile().length();
    final long bytesToRead = Math.min(fileSize - 1, range.getEnd()) - range.getStart() + 1;

    if (bytesToRead < 0 || fileSize < range.getStart()) {
      response.setStatus(REQUESTED_RANGE_NOT_SATISFIABLE.value());
      response.flushBuffer();
      return;
    }

    response.setStatus(PARTIAL_CONTENT.value());
    response.setHeader(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES);
    response.setHeader(HttpHeaders.CONTENT_RANGE,
        String.format("bytes %s-%s/%s",
            range.getStart(), bytesToRead + range.getStart() - 1, s3Object.getSize()));
    response.setHeader(HttpHeaders.ETAG, "\"" + s3Object.getMd5() + "\"");
    response.setDateHeader(HttpHeaders.LAST_MODIFIED, s3Object.getLastModified());

    response.setContentType(s3Object.getContentType());
    response.setContentLengthLong(bytesToRead);
    addUserMetadata(response::addHeader, s3Object);

    try (final OutputStream outputStream = response.getOutputStream()) {
      try (final FileInputStream fis = new FileInputStream(s3Object.getDataFile())) {
        fis.skip(range.getStart());
        IOUtils.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
      }
    }
  }

  private static String filenameFrom(final @PathVariable String bucketName,
      final HttpServletRequest request) {
    final String requestUri = request.getRequestURI();
    return objectNameToFileName(
        UrlEncoded.decodeString(
            requestUri.substring(requestUri.indexOf(bucketName) + bucketName.length() + 1)
        )
    );
  }

  /**
   * Escape object names (and prefixes) so that they can be safely mapped to a file name
   * as consumed by a {@link FileStore}. The encoding should work on at least Unix, Windows
   * and macOS.
   *
   * <p>The escaping is based on a modified URL encoding scheme (using 16 instead of 32 bits)
   * and uses different rules which characters to escape.
   *
   * @param objectName the object name to encode
   * @return encoded key
   */
  // @VisibleForTesting
  static String objectNameToFileName(final String objectName) {
    final char[] chars = objectName.toCharArray();

    final int len = chars.length;
    StringBuffer buffer = null;
    for (int i = 0; i < len; i++) {
      final char c = chars[i];

      // the following characters need escaping
      if (c < ' ' || c >= 0x7f || c == '<' || c == '>' || c == ':' || c == '"' || c == '\\'
          || c == '|' || c == '?' || c == '*' || c == '.' || c == '%') {
        if (buffer == null) {
          buffer = new StringBuffer(objectName.length() * 2);
          buffer.append(objectName, 0, i);
        }

        buffer.append('%');
        TypeUtil.toHex((byte) ((c & 0xff00) >> 8), buffer);
        TypeUtil.toHex((byte) (c & 0xff), buffer);
      } else if (buffer != null) {
        buffer.append(c);
      }
    }

    if (buffer == null) {
      return objectName;
    }

    return buffer.toString();
  }

  // @VisibleForTesting
  static String fileNameToObjectName(final String encoded) {
    StringBuffer buffer = null;

    final char[] chars = encoded.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      final char c = chars[i];

      if (c == '%') {
        if (buffer == null) {
          buffer = new StringBuffer(encoded.length());
          buffer.append(encoded, 0, 0 + i);
        }

        buffer.append((char) TypeUtil.parseInt(encoded, i + 1, 4, 16));
        i += 4;
      } else if (buffer != null) {
        buffer.append(c);
      }
    }

    if (buffer == null) {
      return encoded;
    }

    return buffer.toString();
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

  private S3Object verifyObjectExistence(@PathVariable final String bucketName,
      final String filename) {
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

  private void validateMetadataDirective(final String metadataDirective) {
    if (!(METADATA_DIRECTIVE_REPLACE.equals(metadataDirective)
        || METADATA_DIRECTIVE_COPY.equals(metadataDirective))) {
      throw new S3Exception(BAD_REQUEST.value(), "InvalidRequest",
          "Invalid x-amz-metadata-directive header value.");
    }
  }
}
