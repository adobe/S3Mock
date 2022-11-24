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
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CONTENT_SHA256;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DELETE_MARKER;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ACL;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.DELETE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LEGAL_HOLD;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_ACL;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LEGAL_HOLD;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_RETENTION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOAD_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.RETENTION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING;
import static com.adobe.testing.s3mock.util.HeaderUtil.createEncryptionHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.createOverrideHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.createUserMetadataHeaders;
import static com.adobe.testing.s3mock.util.HeaderUtil.getUserMetadata;
import static com.adobe.testing.s3mock.util.HeaderUtil.isV4ChunkedWithSigningEnabled;
import static com.adobe.testing.s3mock.util.HeaderUtil.parseMediaType;
import static org.springframework.http.HttpHeaders.CONTENT_ENCODING;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopySource;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.ObjectKey;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Range;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective;
import com.adobe.testing.s3mock.util.XmlUtil;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
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
 * Handles requests related to objects.
 */
@CrossOrigin(origins = "*")
@RequestMapping("${com.adobe.testing.s3mock.contextPath:}")
public class ObjectController {
  private static final String RANGES_BYTES = "bytes";

  private final BucketService bucketService;
  private final ObjectService objectService;

  public ObjectController(BucketService bucketService, ObjectService objectService) {
    this.bucketService = bucketService;
    this.objectService = objectService;
  }

  //================================================================================================
  // /{bucketName:.+}
  //================================================================================================

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
      value = "/{bucketName:.+}",
      params = {
          DELETE
      },
      method = RequestMethod.POST,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<DeleteResult> deleteObjects(
      @PathVariable String bucketName,
      @RequestBody Delete body) {
    bucketService.verifyBucketExists(bucketName);
    DeleteResult response = objectService.deleteObjects(bucketName, body);
    return ResponseEntity.ok(response);
  }

  //================================================================================================
  // /{bucketName:.+}/{*key}
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
      value = "/{bucketName:.+}/{*key}",
      method = RequestMethod.HEAD
  )
  public ResponseEntity<Void> headObject(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch) {
    //TODO: needs modified-since handling, see API
    bucketService.verifyBucketExists(bucketName);

    S3ObjectMetadata s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.getKey());
    if (s3ObjectMetadata != null) {
      objectService.verifyObjectMatching(match, noneMatch, s3ObjectMetadata);
      return ResponseEntity.ok()
          .headers(headers -> headers.setAll(createUserMetadataHeaders(s3ObjectMetadata)))
          .headers(headers -> headers.setAll(createEncryptionHeaders(s3ObjectMetadata)))
          .contentType(parseMediaType(s3ObjectMetadata.getContentType()))
          .eTag(s3ObjectMetadata.getEtag())
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
      value = "/{bucketName:.+}/{*key}",
      params = {
          NOT_LIFECYCLE
      },
      method = RequestMethod.DELETE
  )
  public ResponseEntity<Void> deleteObject(@PathVariable String bucketName,
      @PathVariable ObjectKey key) {
    bucketService.verifyBucketExists(bucketName);

    boolean deleted = objectService.deleteObject(bucketName, key.getKey());

    return ResponseEntity.noContent()
        .header(X_AMZ_DELETE_MARKER, String.valueOf(deleted))
        .build();
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
      value = "/{bucketName:.+}/{*key}",
      params = {
          NOT_UPLOADS,
          NOT_UPLOAD_ID,
          NOT_TAGGING,
          NOT_LEGAL_HOLD,
          NOT_RETENTION,
          NOT_ACL
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<StreamingResponseBody> getObject(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = RANGE, required = false) Range range,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestParam Map<String, String> queryParams) {
    //TODO: needs modified-since handling, see API
    bucketService.verifyBucketExists(bucketName);

    S3ObjectMetadata s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.getKey());
    objectService.verifyObjectMatching(match, noneMatch, s3ObjectMetadata);

    if (range != null) {
      return getObjectWithRange(range, s3ObjectMetadata);
    }

    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.getEtag())
        .header(HttpHeaders.CONTENT_ENCODING, s3ObjectMetadata.getContentEncoding())
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .headers(headers -> headers.setAll(createUserMetadataHeaders(s3ObjectMetadata)))
        .headers(headers -> headers.setAll(createEncryptionHeaders(s3ObjectMetadata)))
        .lastModified(s3ObjectMetadata.getLastModified())
        .contentLength(s3ObjectMetadata.getDataPath().toFile().length())
        .contentType(parseMediaType(s3ObjectMetadata.getContentType()))
        .headers(headers -> headers.setAll(createOverrideHeaders(queryParams)))
        .body(outputStream -> Files.copy(s3ObjectMetadata.getDataPath(), outputStream));
  }

  /**
   * Adds an ACL to an object.
   * This method accepts a String instead of the POJO. We need to use JAX-B annotations
   * instead of Jackson annotations because AWS decided to use xsi:type annotations in the XML
   * representation, which are not supported by Jackson.
   * It doesn't seem to be possible to use bot JAX-B and Jackson for (de-)serialization in parallel.
   * :-(
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return {@link ResponseEntity} with Status Code and empty ETag.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          ACL,
      },
      method = RequestMethod.PUT,
      consumes = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Void> putObjectAcl(@PathVariable final String bucketName,
      @PathVariable ObjectKey key,
      @RequestBody String body) throws XMLStreamException, JAXBException {
    bucketService.verifyBucketExists(bucketName);
    objectService.verifyObjectExists(bucketName, key.getKey());
    AccessControlPolicy policy = XmlUtil.deserializeJaxb(body);
    objectService.setAcl(bucketName, key.getKey(), policy);
    return ResponseEntity
        .ok()
        .build();
  }

  /**
   * Gets ACL of an object.
   * This method returns a String instead of the POJO. We need to use JAX-B annotations
   * instead of Jackson annotations because AWS decided to use xsi:type annotations in the XML
   * representation, which are not supported by Jackson.
   * It doesn't seem to be possible to use bot JAX-B and Jackson for (de-)serialization in parallel.
   * :-(
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   *
   * @return {@link ResponseEntity} with Status Code and empty ETag.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          ACL,
      },
      method = RequestMethod.GET,
      produces = {
          APPLICATION_XML_VALUE
      }
  )
  public ResponseEntity<String> getObjectAcl(@PathVariable final String bucketName,
      @PathVariable ObjectKey key) throws JAXBException {
    bucketService.verifyBucketExists(bucketName);
    objectService.verifyObjectExists(bucketName, key.getKey());
    AccessControlPolicy acl = objectService.getAcl(bucketName, key.getKey());
    return ResponseEntity.ok(XmlUtil.serializeJaxb(acl));
  }

  /**
   * Returns the tags identified by bucketName and fileName.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          TAGGING
      },
      method = RequestMethod.GET,
      produces = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Tagging> getObjectTagging(@PathVariable String bucketName,
      @PathVariable ObjectKey key) {
    bucketService.verifyBucketExists(bucketName);

    S3ObjectMetadata s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.getKey());

    List<Tag> tagList = new ArrayList<>(s3ObjectMetadata.getTags());
    Tagging result = new Tagging(tagList);

    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.getEtag())
        .lastModified(s3ObjectMetadata.getLastModified())
        .body(result);
  }

  /**
   * Sets tags for a file identified by bucketName and fileName.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   * @param body Tagging object
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          TAGGING
      },
      method = RequestMethod.PUT,
      consumes = APPLICATION_XML_VALUE
  )
  public ResponseEntity<String> putObjectTagging(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestBody Tagging body) {
    bucketService.verifyBucketExists(bucketName);

    S3ObjectMetadata s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.getKey());
    objectService.setObjectTags(bucketName, key.getKey(), body.getTagSet());
    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.getEtag())
        .lastModified(s3ObjectMetadata.getLastModified())
        .build();
  }

  /**
   * Returns the legal hold for an object.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html">API Reference</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          LEGAL_HOLD
      },
      method = RequestMethod.GET,
      produces = APPLICATION_XML_VALUE
  )
  public ResponseEntity<LegalHold> getLegalHold(@PathVariable String bucketName,
      @PathVariable ObjectKey key) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);
    S3ObjectMetadata s3ObjectMetadata =
        objectService.verifyObjectLockConfiguration(bucketName, key.getKey());

    return ResponseEntity
        .ok()
        .body(s3ObjectMetadata.getLegalHold());
  }

  /**
   * Sets legal hold for an object.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   * @param body legal hold
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          LEGAL_HOLD
      },
      method = RequestMethod.PUT,
      consumes = APPLICATION_XML_VALUE
  )
  public ResponseEntity<String> putLegalHold(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestBody LegalHold body) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);

    objectService.verifyObjectExists(bucketName, key.getKey());
    objectService.setLegalHold(bucketName, key.getKey(), body);
    return ResponseEntity
        .ok()
        .build();
  }

  /**
   * Returns the retention for an object.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html">API Reference</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          RETENTION
      },
      method = RequestMethod.GET,
      produces = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Retention> getObjectRetention(@PathVariable String bucketName,
      @PathVariable ObjectKey key) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);
    S3ObjectMetadata s3ObjectMetadata =
        objectService.verifyObjectLockConfiguration(bucketName, key.getKey());

    return ResponseEntity
        .ok()
        .body(s3ObjectMetadata.getRetention());
  }

  /**
   * Sets retention for an object.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html">API Reference</a>
   *
   * @param bucketName The Bucket's name
   * @param body retention
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          RETENTION
      },
      method = RequestMethod.PUT,
      consumes = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Void> putObjectRetention(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestBody Retention body) {
    bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);

    objectService.verifyObjectExists(bucketName, key.getKey());
    objectService.verifyRetention(body);
    objectService.setRetention(bucketName, key.getKey(), body);
    return ResponseEntity
        .ok()
        .build();
  }

  /**
   * Adds an object to a bucket.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">API Reference</a>
   *
   * @param bucketName the Bucket in which to store the file in.
   * @param encryption The encryption type.
   * @param kmsKeyId The KMS encryption key id.
   *
   * @return {@link ResponseEntity} with Status Code and empty ETag.
   *
   */
  @RequestMapping(
      params = {
          NOT_UPLOAD_ID,
          NOT_TAGGING,
          NOT_LEGAL_HOLD,
          NOT_RETENTION,
          NOT_ACL
      },
      headers = {
          NOT_X_AMZ_COPY_SOURCE
      },
      value = "/{bucketName:.+}/{*key}",
      method = RequestMethod.PUT
  )
  public ResponseEntity<Void> putObject(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) String kmsKeyId,
      @RequestHeader(name = X_AMZ_TAGGING, required = false) List<Tag> tags,
      @RequestHeader(value = CONTENT_ENCODING, required = false) String contentEncoding,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader(value = CONTENT_MD5, required = false) String contentMd5,
      @RequestHeader(value = X_AMZ_CONTENT_SHA256, required = false) String sha256Header,
      @RequestHeader HttpHeaders headers,
      InputStream inputStream) {
    bucketService.verifyBucketExists(bucketName);

    InputStream stream = objectService.verifyMd5(inputStream, contentMd5, sha256Header);
    //TODO: need to extract owner from headers
    Owner owner = Owner.DEFAULT_OWNER;
    Map<String, String> userMetadata = getUserMetadata(headers);
    S3ObjectMetadata s3ObjectMetadata =
        objectService.putS3Object(bucketName,
            key.getKey(),
            parseMediaType(contentType).toString(),
            contentEncoding,
            stream,
            isV4ChunkedWithSigningEnabled(sha256Header),
            userMetadata,
            encryption,
            kmsKeyId,
            tags,
            owner);

    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.getEtag())
        .lastModified(s3ObjectMetadata.getLastModified())
        .header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
        .build();
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
      value = "/{bucketName:.+}/{*key}",
      headers = {
          X_AMZ_COPY_SOURCE
      },
      params = {
          NOT_UPLOAD_ID,
          NOT_TAGGING,
          NOT_LEGAL_HOLD,
          NOT_RETENTION,
          NOT_ACL
      },
      method = RequestMethod.PUT,
      produces = {
          APPLICATION_XML_VALUE
      })
  public ResponseEntity<CopyObjectResult> copyObject(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_COPY_SOURCE) CopySource copySource,
      @RequestHeader(value = X_AMZ_METADATA_DIRECTIVE,
          defaultValue = METADATA_DIRECTIVE_COPY) MetadataDirective metadataDirective,
      @RequestHeader(value = X_AMZ_SERVER_SIDE_ENCRYPTION, required = false) String encryption,
      @RequestHeader(
          value = X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
          required = false) String kmsKeyId,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH,
          required = false) List<String> noneMatch,
      @RequestHeader HttpHeaders httpHeaders) {
    //TODO: needs modified-since handling, see API

    bucketService.verifyBucketExists(bucketName);
    S3ObjectMetadata s3ObjectMetadata =
        objectService.verifyObjectExists(copySource.getBucket(), copySource.getKey());
    objectService.verifyObjectMatchingForCopy(match, noneMatch, s3ObjectMetadata);

    Map<String, String> metadata = Collections.emptyMap();
    if (MetadataDirective.REPLACE == metadataDirective) {
      metadata = getUserMetadata(httpHeaders);
    }

    //TODO: this is potentially illegal on S3. S3 throws a 400:
    // "This copy request is illegal because it is trying to copy an object to itself without
    // changing the object's metadata, storage class, website redirect location or encryption
    // attributes."

    CopyObjectResult copyObjectResult = objectService.copyS3Object(copySource.getBucket(),
        copySource.getKey(),
        bucketName,
        key.getKey(),
        encryption,
        kmsKeyId,
        metadata);

    if (copyObjectResult == null) {
      return ResponseEntity.notFound().header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
          .build();
    }
    return ResponseEntity.ok().header(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, kmsKeyId)
        .body(copyObjectResult);
  }

  /**
   * supports range different range ends. e.g. if content has 100 bytes, the range request could be:
   * bytes=10-100, 10--1 and 10-200
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>
   *
   * @param range {@link String}
   * @param s3ObjectMetadata {@link S3ObjectMetadata}
   */
  private ResponseEntity<StreamingResponseBody> getObjectWithRange(Range range,
      S3ObjectMetadata s3ObjectMetadata) {
    long fileSize = s3ObjectMetadata.getDataPath().toFile().length();
    long bytesToRead = Math.min(fileSize - 1, range.getEnd()) - range.getStart() + 1;

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
        .eTag(s3ObjectMetadata.getEtag())
        .contentType(parseMediaType(s3ObjectMetadata.getContentType()))
        .lastModified(s3ObjectMetadata.getLastModified())
        .contentLength(bytesToRead)
        .body(outputStream -> {
          try (InputStream fis = Files.newInputStream(s3ObjectMetadata.getDataPath())) {
            long skip = fis.skip(range.getStart());
            if (skip == range.getStart()) {
              IOUtils.copy(new BoundedInputStream(fis, bytesToRead), outputStream);
            } else {
              throw new IllegalStateException("Could not skip exact byte range");
            }
          }
        });
  }
}
