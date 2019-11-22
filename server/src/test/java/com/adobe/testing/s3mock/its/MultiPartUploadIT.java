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

package com.adobe.testing.s3mock.its;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;

public class MultiPartUploadIT extends S3TestBase {

  /**
   * Tests if user metadata can be passed by multipart upload.
   */
  @Test
  void shouldPassUserMetadataWithMultipartUploads() {
    s3Client.createBucket(BUCKET_NAME);

    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    final UploadPartResult uploadPartResult = s3Client.uploadPart(new UploadPartRequest()
            .withBucketName(initiateMultipartUploadResult.getBucketName())
            .withKey(initiateMultipartUploadResult.getKey())
            .withUploadId(uploadId)
            .withFile(uploadFile)
            .withFileOffset(0)
            .withPartNumber(1)
            .withPartSize(uploadFile.length())
            .withLastPart(true));

    final List<PartETag> partETags = singletonList(uploadPartResult.getPartETag());
    s3Client.completeMultipartUpload(new CompleteMultipartUploadRequest(
        initiateMultipartUploadResult.getBucketName(),
        initiateMultipartUploadResult.getKey(),
        initiateMultipartUploadResult.getUploadId(),
        partETags
    ));

    final ObjectMetadata metadataExisting = s3Client.getObjectMetadata(
        initiateMultipartUploadResult.getBucketName(), initiateMultipartUploadResult.getKey()
    );

    assertThat("User metadata should be identical!", metadataExisting.getUserMetadata(),
               is(equalTo(objectMetadata.getUserMetadata())));
  }

  @Test
  void shouldInitiateMultipartAndRetrieveParts() throws IOException {
    s3Client.createBucket(BUCKET_NAME);

    final File uploadFile = new File(UPLOAD_FILE_NAME);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    final String hash = DigestUtils.md5Hex(new FileInputStream(uploadFile));
    final String expectedEtag = String.format("%s-%s", hash, 1);
    objectMetadata.addUserMetadata("key", "value");

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(
            new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, objectMetadata));
    final String uploadId = initiateMultipartUploadResult.getUploadId();
    final String key = initiateMultipartUploadResult.getKey();

    s3Client.uploadPart(new UploadPartRequest()
                            .withBucketName(initiateMultipartUploadResult.getBucketName())
                            .withKey(initiateMultipartUploadResult.getKey())
                            .withUploadId(uploadId)
                            .withFile(uploadFile)
                            .withFileOffset(0)
                            .withPartNumber(1)
                            .withPartSize(uploadFile.length())
                            .withLastPart(true));

    final ListPartsRequest listPartsRequest = new ListPartsRequest(
        BUCKET_NAME,
        key,
        uploadId
    );
    final PartListing partListing = s3Client.listParts(listPartsRequest);

    assertThat("Part listing should be 1", partListing.getParts().size(), is(1));
    final PartSummary partSummary = partListing.getParts().get(0);

    assertThat("Etag should match", partSummary.getETag(), is(expectedEtag));
    assertThat("Part number should match", partSummary.getPartNumber(), is(1));
    assertThat("LastModified should be valid date", partSummary.getLastModified(), any(Date.class));

  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed.
   */
  @Test
  void shouldListMultipartUploads() {
    s3Client.createBucket(BUCKET_NAME);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
                       .getMultipartUploads(), is(empty()));

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    final MultipartUploadListing listing =
        s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME));
    assertThat(listing.getMultipartUploads(), is(not(empty())));
    assertThat(listing.getBucketName(), equalTo(BUCKET_NAME));
    assertThat(listing.getMultipartUploads(), hasSize(1));
    final MultipartUpload upload = listing.getMultipartUploads().get(0);
    assertThat(upload.getUploadId(), equalTo(uploadId));
    assertThat(upload.getKey(), equalTo(UPLOAD_FILE_NAME));
  }

  /**
   * Tests if not yet completed / aborted multipart uploads are listed with prefix filtering.
   */
  @Test
  void shouldListMultipartUploadsWithPrefix() {
    s3Client.createBucket(BUCKET_NAME);
    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME, "key1"));
    s3Client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(BUCKET_NAME, "key2"));

    final ListMultipartUploadsRequest listMultipartUploadsRequest =
        new ListMultipartUploadsRequest(BUCKET_NAME);
    listMultipartUploadsRequest.setPrefix("key2");
    final MultipartUploadListing listing
        = s3Client.listMultipartUploads(listMultipartUploadsRequest);

    assertThat(listing.getMultipartUploads(), hasSize(1));
    assertThat(listing.getMultipartUploads().get(0).getKey(), equalTo("key2"));
  }

  /**
   * Tests if a multipart upload can be aborted.
   */
  @Test
  void shouldAbortMultipartUpload() {
    s3Client.createBucket(BUCKET_NAME);

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
                       .getMultipartUploads(), is(empty()));

    final InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME));
    final String uploadId = initiateMultipartUploadResult.getUploadId();

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
                       .getMultipartUploads(), is(not(empty())));

    s3Client.abortMultipartUpload(
        new AbortMultipartUploadRequest(BUCKET_NAME, UPLOAD_FILE_NAME, uploadId));

    assertThat(s3Client.listMultipartUploads(new ListMultipartUploadsRequest(BUCKET_NAME))
                       .getMultipartUploads(), is(empty()));
  }
}
