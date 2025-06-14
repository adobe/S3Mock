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

import static com.adobe.testing.s3mock.S3Exception.NO_SUCH_KEY_DELETE_MARKER;
import static com.adobe.testing.s3mock.dto.StorageClass.STANDARD;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.CONTENT_MD5;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.NOT_X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.RANGE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_ACL;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_CHECKSUM_MODE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_NONE_MATCH;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_COPY_SOURCE_VERSION_ID;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_DELETE_MARKER;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_IF_MATCH_LAST_MODIFIED_TIME;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_IF_MATCH_SIZE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_METADATA_DIRECTIVE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_ATTRIBUTES;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_OBJECT_SIZE;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_STORAGE_CLASS;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpHeaders.X_AMZ_VERSION_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ACL;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.ATTRIBUTES;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.DELETE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.FILE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.KEY;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.LEGAL_HOLD;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_ACL;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_ATTRIBUTES;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_DELETE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LEGAL_HOLD;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_LIFECYCLE;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_RETENTION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOADS;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.NOT_UPLOAD_ID;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.PART_NUMBER;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.RETENTION;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.TAGGING;
import static com.adobe.testing.s3mock.util.AwsHttpParameters.VERSION_ID;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromHeader;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumAlgorithmFromSdk;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.checksumHeaderFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.encryptionHeadersFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.mediaTypeFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.overrideHeadersFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.storageClassHeadersFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.storeHeadersFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.userMetadataFrom;
import static com.adobe.testing.s3mock.util.HeaderUtil.userMetadataHeadersFrom;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpHeaders.IF_MATCH;
import static org.springframework.http.HttpHeaders.IF_MODIFIED_SINCE;
import static org.springframework.http.HttpHeaders.IF_NONE_MATCH;
import static org.springframework.http.HttpHeaders.IF_UNMODIFIED_SINCE;
import static org.springframework.http.HttpStatus.PARTIAL_CONTENT;
import static org.springframework.http.HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import com.adobe.testing.S3Verified;
import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Checksum;
import com.adobe.testing.s3mock.dto.ChecksumAlgorithm;
import com.adobe.testing.s3mock.dto.ChecksumMode;
import com.adobe.testing.s3mock.dto.CopyObjectResult;
import com.adobe.testing.s3mock.dto.CopySource;
import com.adobe.testing.s3mock.dto.Delete;
import com.adobe.testing.s3mock.dto.DeleteResult;
import com.adobe.testing.s3mock.dto.GetObjectAttributesOutput;
import com.adobe.testing.s3mock.dto.LegalHold;
import com.adobe.testing.s3mock.dto.ObjectAttributes;
import com.adobe.testing.s3mock.dto.ObjectCannedACL;
import com.adobe.testing.s3mock.dto.ObjectKey;
import com.adobe.testing.s3mock.dto.Owner;
import com.adobe.testing.s3mock.dto.Retention;
import com.adobe.testing.s3mock.dto.StorageClass;
import com.adobe.testing.s3mock.dto.Tag;
import com.adobe.testing.s3mock.dto.TagSet;
import com.adobe.testing.s3mock.dto.Tagging;
import com.adobe.testing.s3mock.service.BucketService;
import com.adobe.testing.s3mock.service.ObjectService;
import com.adobe.testing.s3mock.store.S3ObjectMetadata;
import com.adobe.testing.s3mock.util.AwsHttpHeaders.MetadataDirective;
import com.adobe.testing.s3mock.util.CannedAclUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BoundedInputStream;
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
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@CrossOrigin(origins = "*", exposedHeaders = "*")
@Controller
@RequestMapping("${com.adobe.testing.s3mock.contextPath:}")
public class ObjectController {
  private static final String RANGES_BYTES = "bytes";
  private static final ObjectMapper XML_MAPPER = new XmlMapper();

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
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html">API Reference</a>.
   */
  @PostMapping(
      value = {
          //AWS SDK V2 pattern
          "/{bucketName:.+}",
          //AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          DELETE
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<DeleteResult> deleteObjects(
      @PathVariable String bucketName,
      @RequestBody Delete body) {
    bucketService.verifyBucketExists(bucketName);
    return ResponseEntity.ok(objectService.deleteObjects(bucketName, body));
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html">API Reference</a>.
   * Does not support all parameters listed in the API reference.
   */
  @PostMapping(
      value = {
          //AWS SDK V2 pattern
          "/{bucketName:.+}",
          //AWS SDK V1 pattern
          "/{bucketName:.+}/"
      },
      params = {
          NOT_DELETE
      },
      produces = APPLICATION_XML_VALUE,
      consumes = MULTIPART_FORM_DATA_VALUE
  )
  public ResponseEntity<Void> postObject(
      @PathVariable String bucketName,
      @RequestParam(value = KEY) ObjectKey key,
      @RequestParam(value = TAGGING, required = false) String tagging,
      @RequestParam(value = CONTENT_TYPE, required = false) String contentType,
      @RequestParam(value = CONTENT_MD5, required = false) String contentMd5,
      @RequestParam(value = X_AMZ_STORAGE_CLASS, required = false) String rawStorageClass,
      @RequestPart(FILE) MultipartFile file) throws IOException {
    List<Tag> tags = null;
    if (tagging != null) {
      Tagging tempTagging = XML_MAPPER.readValue(tagging, Tagging.class);
      if (tempTagging != null && tempTagging.tagSet() != null) {
        tags = tempTagging.tagSet().tags();
      }
    }
    StorageClass storageClass = null;
    if (rawStorageClass != null) {
      storageClass = StorageClass.valueOf(rawStorageClass);
    }

    String checksum = null;
    ChecksumAlgorithm checksumAlgorithm = null;

    var tempFileAndChecksum = objectService.toTempFile(file.getInputStream());

    var bucket = bucketService.verifyBucketExists(bucketName);
    var tempFile = tempFileAndChecksum.getLeft();
    objectService.verifyMd5(tempFile, contentMd5);

    var owner = Owner.DEFAULT_OWNER;
    var s3ObjectMetadata =
        objectService.putS3Object(bucketName,
            key.key(),
            mediaTypeFrom(contentType).toString(),
            Map.of(),
            tempFile,
            Map.of(),
            Map.of(),
            tags,
            checksumAlgorithm,
            checksum,
            owner,
            storageClass);

    FileUtils.deleteQuietly(tempFile.toFile());

    return ResponseEntity
        .ok()
        .headers(h -> h.setAll(checksumHeaderFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(s3ObjectMetadata.encryptionHeaders()))
        .lastModified(s3ObjectMetadata.lastModified())
        .eTag(s3ObjectMetadata.etag())
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .build();
  }

  //================================================================================================
  // /{bucketName:.+}/{*key}
  //================================================================================================

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html">API Reference</a>.
   */
  @RequestMapping(
      value = "/{bucketName:.+}/{*key}",
      method = RequestMethod.HEAD
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> headObject(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestHeader(value = IF_MODIFIED_SINCE, required = false) List<Instant> ifModifiedSince,
      @RequestHeader(value = IF_UNMODIFIED_SINCE, required = false) List<Instant> ifUnmodifiedSince,
      @RequestHeader(value = RANGE, required = false) HttpRange range,
      @RequestParam(value = PART_NUMBER, required = false) String partNumber,
      @RequestParam(value = VERSION_ID, required = false) String versionId,
      @RequestParam Map<String, String> queryParams) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.verifyObjectMatching(match, noneMatch,
        ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata);

    return ResponseEntity.ok()
        .eTag(s3ObjectMetadata.etag())
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .lastModified(s3ObjectMetadata.lastModified())
        .contentLength(Long.parseLong(s3ObjectMetadata.size()))
        .contentType(mediaTypeFrom(s3ObjectMetadata.contentType()))
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .headers(h -> h.setAll(s3ObjectMetadata.storeHeaders()))
        .headers(h -> h.setAll(userMetadataHeadersFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(s3ObjectMetadata.encryptionHeaders()))
        .headers(h -> h.setAll(checksumHeaderFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(storageClassHeadersFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(overrideHeadersFrom(queryParams)))
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html">API Reference</a>.
   */
  @DeleteMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          NOT_LIFECYCLE,
          NOT_TAGGING
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> deleteObject(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = X_AMZ_IF_MATCH_LAST_MODIFIED_TIME, required = false) List<Instant> matchLastModifiedTime,
      @RequestHeader(value = X_AMZ_IF_MATCH_SIZE, required = false) List<Long> matchSize,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    S3ObjectMetadata s3ObjectMetadata = null;
    try {
      s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    } catch (S3Exception e) {
      //ignore NO_SUCH_KEY
    }

    objectService.verifyObjectMatching(match, matchLastModifiedTime, matchSize, s3ObjectMetadata);

    var s3ObjectMetadataVersionId = s3ObjectMetadata != null ? s3ObjectMetadata.versionId() : null;
    var deleted = objectService.deleteObject(bucketName, key.key(), versionId);

    return ResponseEntity.noContent()
        .header(X_AMZ_DELETE_MARKER, String.valueOf(deleted))
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadataVersionId != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadataVersionId);
          }
        })
        .headers(h -> {
          if (bucket.isVersioningEnabled()) {
            try {
              objectService.verifyObjectExists(bucketName, key.key(), versionId);
            } catch (S3Exception e) {
              //ignore all other exceptions here
              if (e == NO_SUCH_KEY_DELETE_MARKER) {
                h.set(X_AMZ_DELETE_MARKER, "true");
              }
            }
          }
        })
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          NOT_UPLOADS,
          NOT_UPLOAD_ID,
          NOT_TAGGING,
          NOT_LEGAL_HOLD,
          NOT_RETENTION,
          NOT_ACL,
          NOT_ATTRIBUTES
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<StreamingResponseBody> getObject(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_CHECKSUM_MODE, required = false, defaultValue = "DISABLED") ChecksumMode mode,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestHeader(value = IF_MODIFIED_SINCE, required = false) List<Instant> ifModifiedSince,
      @RequestHeader(value = IF_UNMODIFIED_SINCE, required = false) List<Instant> ifUnmodifiedSince,
      @RequestParam(value = PART_NUMBER, required = false) String partNumber,
      @RequestHeader(value = RANGE, required = false) HttpRange range,
      @RequestParam(value = VERSION_ID, required = false) String versionId,
      @RequestParam Map<String, String> queryParams) {
    var bucket = bucketService.verifyBucketExists(bucketName);

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.verifyObjectMatching(match, noneMatch,
        ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata);

    if (range != null) {
      return getObjectWithRange(range, s3ObjectMetadata);
    }

    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.etag())
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .lastModified(s3ObjectMetadata.lastModified())
        .contentLength(Long.parseLong(s3ObjectMetadata.size()))
        .contentType(mediaTypeFrom(s3ObjectMetadata.contentType()))
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .headers(h -> h.setAll(s3ObjectMetadata.storeHeaders()))
        .headers(h -> h.setAll(userMetadataHeadersFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(s3ObjectMetadata.encryptionHeaders()))
        .headers(h -> {
          if (mode == ChecksumMode.ENABLED) {
            h.setAll(checksumHeaderFrom(s3ObjectMetadata));
          }
        })
        .headers(h -> h.setAll(storageClassHeadersFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(overrideHeadersFrom(queryParams)))
        .body(outputStream -> Files.copy(s3ObjectMetadata.dataPath(), outputStream));
  }

  /**
   * Adds an ACL to an object.
   * This method accepts a String instead of the POJO. We need to use JAX-B annotations
   * instead of Jackson annotations because AWS decided to use xsi:type annotations in the XML
   * representation, which are not supported by Jackson.
   * It doesn't seem to be possible to use bot JAX-B and Jackson for (de-)serialization in parallel.
   * :-(
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html">API Reference</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html">API Reference</a>
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl">API Reference</a>
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          ACL,
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putObjectAcl(
      @PathVariable final String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_ACL, required = false) ObjectCannedACL cannedAcl,
      @RequestParam(value = VERSION_ID, required = false) String versionId,
      @RequestBody(required = false) AccessControlPolicy body) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    AccessControlPolicy policy;
    if (body != null) {
      policy = body;
    } else if (cannedAcl != null) {
      policy = CannedAclUtil.policyForCannedAcl(cannedAcl);
    } else {
      return ResponseEntity.badRequest().build();
    }
    objectService.setAcl(bucketName, key.key(), versionId, policy);
    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
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
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          ACL,
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<AccessControlPolicy> getObjectAcl(
      @PathVariable final String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    var acl = objectService.getAcl(bucketName, key.key(), versionId);
    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .body(acl);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          TAGGING
      },
      produces = {
          APPLICATION_XML_VALUE,
          APPLICATION_XML_VALUE + ";charset=UTF-8"
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Tagging> getObjectTagging(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);

    Tagging tagging = null;
    if (s3ObjectMetadata.tags() != null && !s3ObjectMetadata.tags().isEmpty()) {
      tagging = new Tagging(new TagSet(s3ObjectMetadata.tags()));
    }

    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.etag())
        .lastModified(s3ObjectMetadata.lastModified())
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .body(tagging);
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html">API Reference</a>.
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          TAGGING
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putObjectTagging(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId,
      @RequestBody Tagging body) {
    var bucket = bucketService.verifyBucketExists(bucketName);

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.setObjectTags(bucketName, key.key(), versionId, body.tagSet().tags());
    return ResponseEntity
        .ok()
        .eTag(s3ObjectMetadata.etag())
        .lastModified(s3ObjectMetadata.lastModified())
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html">API Reference</a>.
   */
  @DeleteMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          TAGGING
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> deleteObjectTagging(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.setObjectTags(bucketName, key.key(), versionId, null);
    return ResponseEntity
        .noContent()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html">API Reference</a>.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          LEGAL_HOLD
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<LegalHold> getLegalHold(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);
    var s3ObjectMetadata = objectService.verifyObjectLockConfiguration(bucketName, key.key(),
        versionId);

    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .body(s3ObjectMetadata.legalHold());
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html">API Reference</a>.
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          LEGAL_HOLD
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putLegalHold(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId,
      @RequestBody LegalHold body) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.setLegalHold(bucketName, key.key(), versionId, body);
    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html">API Reference</a>.
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          RETENTION
      },
      produces = APPLICATION_XML_VALUE
  )
  public ResponseEntity<Retention> getObjectRetention(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);
    var s3ObjectMetadata = objectService.verifyObjectLockConfiguration(bucketName, key.key(),
        versionId);

    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .body(s3ObjectMetadata.retention());
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html">API Reference</a>.
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          RETENTION
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putObjectRetention(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestParam(value = VERSION_ID, required = false) String versionId,
      @RequestBody Retention body) {
    var bucket = bucketService.verifyBucketExists(bucketName);
    bucketService.verifyBucketObjectLockEnabled(bucketName);

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.verifyRetention(body);
    objectService.setRetention(bucketName, key.key(), versionId, body);
    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html">API Reference</a>.
   */
  @GetMapping(
      value = "/{bucketName:[a-z0-9.-]+}/{*key}",
      params = {
          ATTRIBUTES
      },
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<GetObjectAttributesOutput> getObjectAttributes(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestHeader(value = IF_MODIFIED_SINCE, required = false) List<Instant> ifModifiedSince,
      @RequestHeader(value = IF_UNMODIFIED_SINCE, required = false) List<Instant> ifUnmodifiedSince,
      @RequestHeader(value = X_AMZ_OBJECT_ATTRIBUTES) List<String> objectAttributes,
      @RequestParam(value = VERSION_ID, required = false) String versionId) {
    var bucket = bucketService.verifyBucketExists(bucketName);

    //this is for either an object request, or a parts request.

    var s3ObjectMetadata = objectService.verifyObjectExists(bucketName, key.key(), versionId);
    objectService.verifyObjectMatching(match, noneMatch,
        ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata);
    //S3Mock stores the etag with the additional quotation marks needed in the headers. This
    // response does not use eTag as a header, so it must not contain the quotation marks.
    var etag = s3ObjectMetadata.etag().replace("\"", "");
    var objectSize = Long.parseLong(s3ObjectMetadata.size());
    //in object attributes, S3 returns STANDARD, in all other APIs it returns null...
    var storageClass = s3ObjectMetadata.storageClass() == null
        ? STANDARD
        : s3ObjectMetadata.storageClass();
    var response = new GetObjectAttributesOutput(
        Checksum.from(s3ObjectMetadata),
        objectAttributes.contains(ObjectAttributes.ETAG.toString())
            ? etag
            : null,
        null, //parts not supported right now
        objectAttributes.contains(ObjectAttributes.OBJECT_SIZE.toString())
            ? objectSize
            : null,
        objectAttributes.contains(ObjectAttributes.STORAGE_CLASS.toString())
            ? storageClass
            : null
    );

    return ResponseEntity
        .ok()
        .lastModified(s3ObjectMetadata.lastModified())
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .body(response);
  }


  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html">API Reference</a>.
   */
  @PutMapping(
      value = "/{bucketName:.+}/{*key}",
      params = {
          NOT_UPLOAD_ID,
          NOT_TAGGING,
          NOT_LEGAL_HOLD,
          NOT_RETENTION,
          NOT_ACL
      },
      headers = {
          NOT_X_AMZ_COPY_SOURCE
      }
  )
  @S3Verified(year = 2025)
  public ResponseEntity<Void> putObject(
      @PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_TAGGING, required = false) List<Tag> tags,
      @RequestHeader(value = CONTENT_TYPE, required = false) String contentType,
      @RequestHeader(value = CONTENT_MD5, required = false) String contentMd5,
      @RequestHeader(value = IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestHeader(value = X_AMZ_STORAGE_CLASS, required = false,
          defaultValue = "STANDARD") StorageClass storageClass,
      @RequestHeader HttpHeaders httpHeaders,
      InputStream inputStream) {

    String checksum = null;
    ChecksumAlgorithm checksumAlgorithm = null;

    var tempFileAndChecksum = objectService.toTempFile(inputStream, httpHeaders);

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
    final var bucket = bucketService.verifyBucketExists(bucketName);
    objectService.verifyObjectMatching(bucketName, key.key(), match, noneMatch);
    var tempFile = tempFileAndChecksum.getLeft();
    objectService.verifyMd5(tempFile, contentMd5);
    if (checksum != null) {
      objectService.verifyChecksum(tempFile, checksum, checksumAlgorithm);
    }

    var owner = Owner.DEFAULT_OWNER;
    var s3ObjectMetadata =
        objectService.putS3Object(bucketName,
            key.key(),
            mediaTypeFrom(contentType).toString(),
            storeHeadersFrom(httpHeaders),
            tempFile,
            userMetadataFrom(httpHeaders),
            encryptionHeadersFrom(httpHeaders),
            tags,
            checksumAlgorithm,
            checksum,
            owner,
            storageClass);

    FileUtils.deleteQuietly(tempFile.toFile());

    return ResponseEntity
        .ok()
        .headers(h -> {
          if (bucket.isVersioningEnabled() && s3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, s3ObjectMetadata.versionId());
          }
        })
        .headers(h -> h.setAll(checksumHeaderFrom(s3ObjectMetadata)))
        .headers(h -> h.setAll(s3ObjectMetadata.encryptionHeaders()))
        .header(X_AMZ_OBJECT_SIZE, s3ObjectMetadata.size())
        .lastModified(s3ObjectMetadata.lastModified())
        .eTag(s3ObjectMetadata.etag())
        .build();
  }

  /**
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">API Reference</a>.
   */
  @PutMapping(
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
      produces = APPLICATION_XML_VALUE
  )
  @S3Verified(year = 2025)
  public ResponseEntity<CopyObjectResult> copyObject(@PathVariable String bucketName,
      @PathVariable ObjectKey key,
      @RequestHeader(value = X_AMZ_COPY_SOURCE) CopySource copySource,
      @RequestHeader(value = X_AMZ_METADATA_DIRECTIVE, defaultValue = "COPY") MetadataDirective metadataDirective,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MATCH, required = false) List<String> match,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_NONE_MATCH, required = false) List<String> noneMatch,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE, required = false) List<Instant> ifModifiedSince,
      @RequestHeader(value = X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE, required = false) List<Instant> ifUnmodifiedSince,
      @RequestHeader(value = X_AMZ_STORAGE_CLASS, required = false) StorageClass storageClass,
      @RequestHeader HttpHeaders httpHeaders) {
    var targetBucket = bucketService.verifyBucketExists(bucketName);
    var sourceBucket = bucketService.verifyBucketExists(copySource.bucket());
    var s3ObjectMetadata = objectService.verifyObjectExists(copySource.bucket(), copySource.key(),
        copySource.versionId());
    objectService.verifyObjectMatchingForCopy(match, noneMatch,
        ifModifiedSince, ifUnmodifiedSince, s3ObjectMetadata);

    var userMetadata = Collections.<String, String>emptyMap();
    var storeHeaders = Collections.<String, String>emptyMap();
    if (MetadataDirective.REPLACE == metadataDirective) {
      userMetadata = userMetadataFrom(httpHeaders);
      storeHeaders = storeHeadersFrom(httpHeaders);
    }

    var copyS3ObjectMetadata = objectService.copyS3Object(copySource.bucket(),
        copySource.key(),
        copySource.versionId(),
        bucketName,
        key.key(),
        encryptionHeadersFrom(httpHeaders),
        storeHeaders,
        userMetadata,
        storageClass);

    //return expiration

    if (copyS3ObjectMetadata == null) {
      return ResponseEntity
          .notFound()
          .headers(headers -> headers.setAll(s3ObjectMetadata.encryptionHeaders()))
          .build();
    }
    return ResponseEntity
        .ok()
        .headers(headers -> headers.setAll(s3ObjectMetadata.encryptionHeaders()))
        .headers(h -> {
          if (sourceBucket.isVersioningEnabled() && copySource.versionId() != null) {
            h.set(X_AMZ_COPY_SOURCE_VERSION_ID, copySource.versionId());
          }
        }).headers(h -> {
          if (targetBucket.isVersioningEnabled() && copyS3ObjectMetadata.versionId() != null) {
            h.set(X_AMZ_VERSION_ID, copyS3ObjectMetadata.versionId());
          }
        })
        .body(new CopyObjectResult(copyS3ObjectMetadata));
  }

  /**
   * Supports returning different ranges of an object.
   * E.g., if content has 100 bytes, the range request could be: bytes=10-100, 10--1 and 10-200
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html">API Reference</a>
   *
   * @param range {@link String}
   * @param s3ObjectMetadata {@link S3ObjectMetadata}
   */
  private ResponseEntity<StreamingResponseBody> getObjectWithRange(HttpRange range,
      S3ObjectMetadata s3ObjectMetadata) {
    var fileSize = s3ObjectMetadata.dataPath().toFile().length();
    var bytesToRead = Math.min(fileSize - 1, range.getRangeEnd(fileSize))
        - range.getRangeStart(fileSize) + 1;

    if (bytesToRead < 0 || fileSize < range.getRangeStart(fileSize)) {
      return ResponseEntity.status(REQUESTED_RANGE_NOT_SATISFIABLE.value()).build();
    }

    return ResponseEntity
        .status(PARTIAL_CONTENT.value())
        .headers(headers -> headers.setAll(userMetadataHeadersFrom(s3ObjectMetadata)))
        .headers(headers -> headers.setAll(s3ObjectMetadata.storeHeaders()))
        .headers(headers -> headers.setAll(s3ObjectMetadata.encryptionHeaders()))
        .header(HttpHeaders.ACCEPT_RANGES, RANGES_BYTES)
        .header(HttpHeaders.CONTENT_RANGE,
            String.format("bytes %s-%s/%s",
                range.getRangeStart(fileSize), bytesToRead + range.getRangeStart(fileSize) - 1,
                s3ObjectMetadata.size()))
        .eTag(s3ObjectMetadata.etag())
        .contentType(mediaTypeFrom(s3ObjectMetadata.contentType()))
        .lastModified(s3ObjectMetadata.lastModified())
        .contentLength(bytesToRead)
        .body(outputStream ->
            extractBytesToOutputStream(range, s3ObjectMetadata, outputStream, fileSize, bytesToRead)
        );
  }

  private static void extractBytesToOutputStream(HttpRange range, S3ObjectMetadata s3ObjectMetadata,
      OutputStream outputStream, long fileSize, long bytesToRead) throws IOException {
    try (var fis = Files.newInputStream(s3ObjectMetadata.dataPath())) {
      var skip = fis.skip(range.getRangeStart(fileSize));
      if (skip == range.getRangeStart(fileSize)) {
        try (var bis = BoundedInputStream
            .builder()
            .setInputStream(fis)
            .setMaxCount(bytesToRead)
            .get()) {
          bis.transferTo(outputStream);
        }
      } else {
        throw new IllegalStateException("Could not skip exact byte range");
      }
    }
  }
}
