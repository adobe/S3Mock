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
import static com.adobe.testing.s3mock.util.HeaderUtil.createEncryptionHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.createOverrideHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.createUserMetadataHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.getUserMetadata;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;
import static com.adobe.testing.s3mock.util.HeaderUtil.parseMediaType;
import static com.adobe.testing.s3mock.util.StringEncoding.urlEncodeIgnoreSlashes;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.CompleteMultipartUpload;
import com.adobe.testing.s3mock.dto.CompleteMultipartUploadResult;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopyPartResult;
import com.adobe.testing.s3mock.dto.CopySource;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.DeletedS3Object;
import com.adobe.testing.s3mock.dto.InitiateMultipartUploadResult;
import com.adobe.testing.s3mock.dto.ListBucketResult;
import com.adobe.testing.s3mock.dto.ListBucketResultV2;
import com.adobe.testing.s3mock.dto.ListMultipartUploadsResult;
import com.adobe.testing.s3mock.dto.ListPartsResult;
import com.adobe.testing.s3mock.dto.MultipartUpload;
import com.adobe.testing.s3mock.dto.ObjectKey;
import com.adobe.testing.s3mock.dto.Part;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.S3Object;
import com.adobe.testing.s3mock.dto.S3ObjectIdentifier;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.store.BucketStore;
import com.adobe.testing.s3mock.store.FileStore;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective;
import com.adobe.testing.s3mock.util.StringEncoding;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
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
public class FileStoreController extends ControllerBase {
  private static final String RANGES_BYTES = "bytes";

  private static final Logger LOG = LoggerFactory.getLogger(FileStoreController.class);

  private static final Comparator<String> KEY_COMPARATOR = Comparator.naturalOrder();
  private static final Comparator<S3Object> BUCKET_CONTENTS_COMPARATOR =
      Comparator.comparing(S3Object::getKey, KEY_COMPARATOR);

  private final Map<String, String> fileStorePagingStateCache = new ConcurrentHashMap<>();

  public FileStoreController(FileStore fileStore, BucketStore bucketStore) {
    super(fileStore, bucketStore);
  }

  //================================================================================================
  // /{bucketName:[a-z0-9.-]+}
  //================================================================================================

  /**
   * Retrieve list of objects of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html">API Reference</a>
   *
   * @param bucketName {@link String} set bucket name
   * @param prefix {@link String} find object names they start with prefix
   * @param encodingType whether to use URL encoding (encodingtype="url") or not
   *
   * @return {@link ListBucketResult} a list of objects in Bucket
   * @deprecated Long since replaced by ListObjectsV2, {@see #listObjectsInsideBucketV2}
   */
  @RequestMapping(
      params = {
          NOT_UPLOADS
      },
      value = "/{bucketName:[a-z0-9.-]+}",
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  @Deprecated
  public ResponseEntity<ListBucketResult> listObjects(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(required = false) final String marker,
      @RequestParam(name = ENCODING_TYPE, required = false) final String encodingType,
      @RequestParam(name = MAX_KEYS, defaultValue = "1000",
          required = false) final Integer maxKeys) {
    verifyBucketExists(bucketName);
    verifyMaxKeys(maxKeys);
    verifyEncodingType(encodingType);

    final boolean useUrlEncoding = Objects.equals("url", encodingType);

    List<S3Object> contents = getBucketContents(bucketName, prefix);
    contents = filterBucketContentsBy(contents, marker);

    boolean isTruncated = false;
    String nextMarker = null;

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents);
    contents = filterBucketContentsBy(contents, commonPrefixes);
    if (maxKeys < contents.size()) {
      contents = contents.subList(0, maxKeys);
      isTruncated = true;
      if (maxKeys > 0) {
        nextMarker = contents.get(maxKeys - 1).getKey();
      }
    }

    String returnPrefix = prefix;
    List<String> returnCommonPrefixes = commonPrefixes;

    if (useUrlEncoding) {
      contents = apply(contents, (object) -> {
        object.setKey(urlEncodeIgnoreSlashes(object.getKey()));
        return object;
      });
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnCommonPrefixes = apply(commonPrefixes, StringEncoding::urlEncodeIgnoreSlashes);
    }

    return ResponseEntity.ok(
        new ListBucketResult(bucketName, returnPrefix, marker, maxKeys, isTruncated,
            encodingType, nextMarker, contents, returnCommonPrefixes));
  }

  /**
   * Retrieve list of objects of a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>
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
  @RequestMapping(value = "/{bucketName:[a-z0-9.-]+}",
      params = {
          LIST_TYPE_V2
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListBucketResultV2> listObjectsV2(
      @PathVariable final String bucketName,
      @RequestParam(required = false) final String prefix,
      @RequestParam(required = false) final String delimiter,
      @RequestParam(name = ENCODING_TYPE, required = false) final String encodingtype,
      @RequestParam(name = START_AFTER, required = false) final String startAfter,
      @RequestParam(name = MAX_KEYS,
          defaultValue = "1000", required = false) final Integer maxKeys,
      @RequestParam(name = CONTINUATION_TOKEN, required = false) final String continuationToken) {
    verifyMaxKeys(maxKeys);
    verifyEncodingType(encodingtype);

    final boolean useUrlEncoding = Objects.equals("url", encodingtype);

    verifyBucketExists(bucketName);
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

    List<String> commonPrefixes = collapseCommonPrefixes(prefix, delimiter, contents);
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
    List<String> returnCommonPrefixes = commonPrefixes;

    if (useUrlEncoding) {
      contents = apply(contents, (object) -> {
        String key = object.getKey();
        object.setKey(urlEncodeIgnoreSlashes(key));
        return object;
      });
      returnPrefix = urlEncodeIgnoreSlashes(prefix);
      returnStartAfter = urlEncodeIgnoreSlashes(startAfter);
      returnCommonPrefixes = apply(commonPrefixes, StringEncoding::urlEncodeIgnoreSlashes);
    }

    return ResponseEntity.ok(new ListBucketResultV2(bucketName, returnPrefix, maxKeys,
        isTruncated, contents, returnCommonPrefixes,
        continuationToken, String.valueOf(contents.size()),
        nextContinuationToken, returnStartAfter, encodingtype));
  }

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
          "/{bucketName:[a-z0-9.-]+}",
          //AWS SDK V1 pattern
          "/{bucketName:[a-z0-9.-]+}/"
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
      @RequestParam(required = false) final String prefix) {
    verifyBucketExists(bucketName);

    final List<MultipartUpload> multipartUploads =
        fileStore.listMultipartUploads(bucketName).stream()
            .filter(m -> isBlank(prefix) || m.getKey().startsWith(prefix))
            .map(m -> new MultipartUpload(m.getKey(), m.getUploadId(),
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
   * This operation removes multiple objects.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">API Reference</a>
   *
   * @param bucketName name of bucket containing the object.
   * @param body The delete request.
   *
   * @return The {@link DeleteResult}
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}",
      params = {
          DELETE
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<DeleteResult> deleteObjects(
      @PathVariable final String bucketName,
      @RequestBody final Delete body) {
    verifyBucketExists(bucketName);
    DeleteResult response = new DeleteResult();
    for (final S3ObjectIdentifier object : body.getObjectsToDelete()) {
      try {
        if (fileStore.deleteObject(bucketName, object.getKey())) {
          response.addDeletedObject(DeletedS3Object.from(object));
        } else {
          //TODO: There may be different error reasons than a non-existent key.
          response.addError(
              new com.adobe.testing.s3mock.dto.Error("NoSuchKey",
                  object.getKey(),
                  "The specified key does not exist.",
                  object.getVersionId()));
        }
      } catch (final IllegalStateException e) {
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
  // /{bucketName:[a-z0-9.-]+}/{*key}
  //================================================================================================

  /**
   * Retrieves metadata from an object without returning the object itself.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html">API Reference</a>
   *
   * @param bucketName name of the bucket to look in
   *
   * @return 200 with object metadata headers, 404 if not found.
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      method = RequestMethod.HEAD
  )
  public ResponseEntity<Void> headObject(@PathVariable final String bucketName,
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);

    final S3ObjectMetadata s3ObjectMetadata = fileStore.getS3Object(bucketName, key.getKey());
    if (s3ObjectMetadata != null) {
      return ResponseEntity.ok()
          .headers(headers -> headers.setAll(createUserMetadataHeaders(s3ObjectMetadata)))
          .headers(headers -> headers.setAll(createEncryptionHeaders(s3ObjectMetadata)))
          .contentType(parseMediaType(s3ObjectMetadata.getContentType()))
          .eTag("\"" + s3ObjectMetadata.getEtag() + "\"")
          .contentLength(Long.parseLong(s3ObjectMetadata.getSize()))
          .lastModified(s3ObjectMetadata.getLastModified())
          .build();
    } else {
      return ResponseEntity.status(NOT_FOUND).build();
    }
  }

  /**
   * The DELETE operation removes an object.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html">API Reference</a>
   *
   * @param bucketName name of bucket containing the object.
   *
   * @return ResponseEntity with Status Code 204 if object was successfully deleted.
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      method = RequestMethod.DELETE
  )
  public ResponseEntity<Void> deleteObject(@PathVariable final String bucketName,
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);

    fileStore.deleteObject(bucketName, key.getKey());

    return ResponseEntity.noContent().build();
  }

  /**
   * Aborts a multipart upload for a given uploadId.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param uploadId id of the upload. Has to match all other part's uploads.
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);

    fileStore.abortMultipartUpload(bucketName, key.getKey(), uploadId);
    return ResponseEntity.noContent().build();
  }

  /**
   * Returns the File identified by bucketName and fileName.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   * @param range byte range
   *
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
      @PathVariable ObjectKey key,
      HttpServletRequest request) {
    verifyBucketExists(bucketName);

    S3ObjectMetadata s3ObjectMetadata = verifyObjectExistence(bucketName, key.getKey());
    verifyObjectMatching(match, noMatch, s3ObjectMetadata.getEtag());

    if (range != null) {
      return getObjectWithRange(range, s3ObjectMetadata);
    }

    return ResponseEntity
        .ok()
        .eTag("\"" + s3ObjectMetadata.getEtag() + "\"")
        .header(HttpHeaders.CONTENT_ENCODING, s3ObjectMetadata.getContentEncoding())
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .headers(headers -> headers.setAll(createUserMetadataHeaders(s3ObjectMetadata)))
        .headers(headers -> headers.setAll(createEncryptionHeaders(s3ObjectMetadata)))
        .lastModified(s3ObjectMetadata.getLastModified())
        .contentLength(s3ObjectMetadata.getDataPath().toFile().length())
        .contentType(parseMediaType(s3ObjectMetadata.getContentType()))
        .headers(headers -> headers.setAll(createOverrideHeaders(request.getQueryString())))
        .body(outputStream -> Files.copy(s3ObjectMetadata.getDataPath(), outputStream));
  }

  /**
   * Returns the tags identified by bucketName and fileName.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      params = {
          TAGGING
      },
      method = RequestMethod.GET,
      produces = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Tagging> getObjectTagging(@PathVariable final String bucketName,
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);

    final S3ObjectMetadata s3ObjectMetadata = verifyObjectExistence(bucketName, key.getKey());

    final List<Tag> tagList = new ArrayList<>(s3ObjectMetadata.getTags());
    final Tagging result = new Tagging(tagList);

    return ResponseEntity
        .ok()
        .eTag("\"" + s3ObjectMetadata.getEtag() + "\"")
        .lastModified(s3ObjectMetadata.getLastModified())
        .body(result);
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
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      params = {
          UPLOAD_ID
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<ListPartsResult> listParts(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);
    verifyMultipartUploadExists(uploadId);

    final List<Part> parts = fileStore.getMultipartUploadParts(bucketName, key.getKey(), uploadId);
    return ResponseEntity.ok(new ListPartsResult(bucketName, key.getKey(), uploadId, parts));
  }

  /**
   * Sets tags for a file identified by bucketName and fileName.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   * @param body Tagging object
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      params = {
          TAGGING
      },
      method = RequestMethod.PUT
  )
  public ResponseEntity<String> putObjectTagging(@PathVariable final String bucketName,
      @RequestBody final Tagging body,
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);

    S3ObjectMetadata s3ObjectMetadata = verifyObjectExistence(bucketName, key.getKey());
    fileStore.setObjectTags(bucketName, key.getKey(), body.getTagSet());
    return ResponseEntity
        .ok()
        .eTag("\"" + s3ObjectMetadata.getEtag() + "\"")
        .lastModified(s3ObjectMetadata.getLastModified())
        .build();
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
   * @param request {@link HttpServletRequest} of this request.
   *
   * @return the etag of the uploaded part.
   *
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
  public ResponseEntity<Void> uploadPart(@PathVariable final String bucketName,
      @RequestParam final String uploadId,
      @RequestParam final String partNumber,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false)
      final String encryption,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, required = false)
      final String kmsKeyId,
      @RequestHeader(value = X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      @PathVariable ObjectKey key,
      HttpServletRequest request) throws IOException {
    verifyBucketExists(bucketName);
    verifyPartNumberLimits(partNumber);

    final String etag = fileStore.putPart(bucketName,
        key.getKey(),
        uploadId,
        partNumber,
        request.getInputStream(),
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
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
      @PathVariable ObjectKey key) {
    verifyBucketExists(bucketName);
    verifyObjectExistence(copySource.getBucket(), copySource.getKey());
    final String partEtag = fileStore.copyPart(copySource.getBucket(),
        copySource.getKey(),
        copyRange,
        partNumber,
        bucketName,
        key.getKey(),
        uploadId
    );

    return ResponseEntity.ok(CopyPartResult.from(new Date(), "\"" + partEtag + "\""));
  }

  /**
   * Adds an object to a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param encryption The encryption type.
   * @param kmsKeyId The KMS encryption key id.
   * @param request http servlet request.
   *
   * @return {@link ResponseEntity} with Status Code and empty ETag.
   *
   */
  @RequestMapping(
      params = {
          NOT_UPLOAD_ID,
          NOT_TAGGING
      },
      headers = {
          NOT_X_AMZ_COPY_SOURCE
      },
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
      @PathVariable ObjectKey key,
      HttpServletRequest request) {
    verifyBucketExists(bucketName);

    final S3ObjectMetadata s3ObjectMetadata;
    try (ServletInputStream inputStream = request.getInputStream()) {
      InputStream stream = verifyMd5(inputStream, contentMd5, sha256Header);
      final Map<String, String> userMetadata = getUserMetadata(request);
      s3ObjectMetadata =
          fileStore.putS3Object(bucketName,
              key.getKey(),
              parseMediaType(contentType).toString(),
              contentEncoding,
              stream,
              isV4ChunkedWithSigningEnabled(sha256Header),
              userMetadata,
              encryption,
              kmsKeyId);

      fileStore.setObjectTags(bucketName, key.getKey(), tags);

      return ResponseEntity
          .ok()
          .eTag("\"" + s3ObjectMetadata.getEtag() + "\"")
          .lastModified(s3ObjectMetadata.getLastModified())
          .header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
          .build();
    } catch (final IOException e) {
      LOG.error("Object could not be uploaded!", e);
      throw new IllegalStateException("Object could not be uploaded!", e);
    }
  }

  /**
   * Copies an object to another bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">API Reference</a>
   *
   * @param bucketName name of the destination bucket
   * @param copySource path to source object
   * @param encryption The Encryption Type
   * @param kmsKeyId The KMS encryption key id
   *
   * @return {@link CopyObjectResult}
   *
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
      @PathVariable ObjectKey key,
      HttpServletRequest request) {
    verifyBucketExists(bucketName);
    verifyObjectExistence(copySource.getBucket(), copySource.getKey());

    final CopyObjectResult copyObjectResult;
    if (MetadataDirective.REPLACE == metadataDirective) {
      copyObjectResult = fileStore.copyS3Object(copySource.getBucket(),
          copySource.getKey(),
          bucketName,
          key.getKey(),
          encryption,
          kmsKeyId,
          getUserMetadata(request));
    } else {
      copyObjectResult = fileStore.copyS3Object(copySource.getBucket(),
          copySource.getKey(),
          bucketName,
          key.getKey(),
          encryption,
          kmsKeyId,
          Collections.emptyMap());
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
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return the {@link InitiateMultipartUploadResult}.
   */
  @RequestMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      params = {
          UPLOADS
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<InitiateMultipartUploadResult> createMultipartUpload(
      @PathVariable final String bucketName,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, required = false)
      String kmsKeyId,
      @PathVariable ObjectKey key,
      HttpServletRequest request) {
    verifyBucketExists(bucketName);

    final Map<String, String> userMetadata = getUserMetadata(request);

    final String uploadId = UUID.randomUUID().toString();
    fileStore.prepareMultipartUpload(bucketName, key.getKey(),
        parseMediaType(request.getContentType()).toString(),
        request.getHeader(HttpHeaders.CONTENT_ENCODING), uploadId,
        TEST_OWNER, TEST_OWNER, userMetadata);

    return ResponseEntity.ok(
        new InitiateMultipartUploadResult(bucketName, key.getKey(), uploadId));
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
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
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
      @RequestBody final CompleteMultipartUpload requestBody,
      @PathVariable ObjectKey key,
      HttpServletRequest request) {
    verifyBucketExists(bucketName);
    validateMultipartParts(bucketName, key.getKey(), uploadId, requestBody.getParts());
    final String eTag = fileStore.completeMultipartUpload(bucketName,
        key.getKey(),
        uploadId,
        requestBody.getParts(),
        encryption,
        kmsKeyId);

    return ResponseEntity.ok(
        new CompleteMultipartUploadResult(request.getRequestURL().toString(), bucketName,
            key.getKey(), eTag));
  }

  /**
   * supports range different range ends. e.g. if content has 100 bytes, the range request could be:
   * bytes=10-100, 10--1 and 10-200
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>
   *
   * @param range {@link String}
   * @param s3ObjectMetadata {@link S3ObjectMetadata}
   */
  private ResponseEntity<StreamingResponseBody> getObjectWithRange(final Range range,
      final S3ObjectMetadata s3ObjectMetadata) {
    final long fileSize = s3ObjectMetadata.getDataPath().toFile().length();
    final long bytesToRead = Math.min(fileSize - 1, range.getEnd()) - range.getStart() + 1;

    if (bytesToRead < 0 || fileSize < range.getStart()) {
      return ResponseEntity.status(REQUESTED_RANGE_NOT_SATISFIABLE.value()).build();
    }

    return ResponseEntity
        .status(PARTIAL_CONTENT.value())
        .headers(headers -> headers.setAll(createUserMetadataHeaders(s3ObjectMetadata)))
        .headers(headers -> headers.setAll(createEncryptionHeaders(s3ObjectMetadata)))
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .header(HttpHeaders.CONTENT_RANGE,
            String.format("bytes %s-%s/%s",
                range.getStart(), bytesToRead + range.getStart() - 1, s3ObjectMetadata.getSize()))
        .eTag("\"" + s3ObjectMetadata.getEtag() + "\"")
        .contentType(parseMediaType(s3ObjectMetadata.getContentType()))
        .lastModified(s3ObjectMetadata.getLastModified())
        .contentLength(bytesToRead)
        .body(outputStream -> {
          try (FileInputStream fis = new FileInputStream(s3ObjectMetadata.getDataPath().toFile())) {
            fis.skip(range.getStart());
            IOUtils.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
          }
        });
  }

  /**
   * Collapse all bucket elements with keys starting with some prefix up to the given delimiter into
   * one prefix entry. Collapsed elements are removed from the contents list.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html">API Reference</a>
   *
   * @param queryPrefix the key prefix as specified in the list request
   * @param delimiter the delimiter used to separate a prefix from the rest of the object name
   * @param contents the contents list
   */
  static List<String> collapseCommonPrefixes(final String queryPrefix, final String delimiter,
      final List<S3Object> contents) {
    final List<String> commonPrefixes = new ArrayList<>();
    if (isEmpty(delimiter)) {
      return commonPrefixes;
    }

    final String normalizedQueryPrefix = queryPrefix == null ? "" : queryPrefix;

    for (S3Object c : contents) {
      final String key = c.getKey();
      if (key.startsWith(normalizedQueryPrefix)) {
        final int delimiterIndex = key.indexOf(delimiter, normalizedQueryPrefix.length());
        if (delimiterIndex > 0) {
          String commonPrefix = key.substring(0, delimiterIndex + delimiter.length());
          if (!commonPrefixes.contains(commonPrefix)) {
            commonPrefixes.add(commonPrefix);
          }
        }
      }
    }
    return commonPrefixes;
  }

  private static <T> List<T> apply(List<T> contents, Function<T, T> extractor) {
    return contents
        .stream()
        .map(extractor)
        .collect(Collectors.toList());
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
      List<String> commonPrefixes) {
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

  private List<S3Object> getBucketContents(String bucketName, String prefix) {

    List<S3ObjectMetadata> s3ObjectMetadata = fileStore.getS3Objects(bucketName, prefix);

    LOG.debug("Found {} objects in bucket {}", s3ObjectMetadata.size(), bucketName);
    return s3ObjectMetadata.stream().map(s3Object -> new S3Object(
            s3Object.getName(),
            s3Object.getModificationDate(), s3Object.getEtag(),
            s3Object.getSize(), StorageClass.STANDARD, TEST_OWNER))
        // List Objects results are expected to be sorted by key
        .sorted(BUCKET_CONTENTS_COMPARATOR)
        .collect(Collectors.toList());
  }
}
