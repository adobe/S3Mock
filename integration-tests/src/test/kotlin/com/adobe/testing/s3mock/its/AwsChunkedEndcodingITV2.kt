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

package com.adobe.testing.s3mock.its

import com.adobe.testing.s3mock.util.DigestUtil
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.File
import java.io.FileInputStream
import java.io.InputStream

/**
 * Chunked encoding with signing is only active in AWS SDK v2 when endpoint is http
 */
internal class AwsChunkedEndcodingITV2 : S3TestBase() {

    private val client = createS3ClientV2(serviceEndpointHttp)

    /**
     * Unfortunately the S3 API does not persist or return data that would let us verify if signed and chunked encoding
     * was actually used for the putObject request.
     * This was manually validated through the debugger.
     */
    @Test
    @S3VerifiedFailure(year = 2023,
        reason = "Only works with http endpoints")
    fun testPutObject_etagCreation(testInfo: TestInfo) {
        val bucket = givenBucketV2(testInfo)
        val uploadFile = File(UPLOAD_FILE_NAME)
        val uploadFileIs: InputStream = FileInputStream(uploadFile)
        val expectedEtag = "\"${DigestUtil.hexDigest(uploadFileIs)}\""

        client.putObject(
            PutObjectRequest.builder()
                .bucket(bucket)
                .key(UPLOAD_FILE_NAME)
              .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                .build(),
            RequestBody.fromFile(uploadFile))

        val getObjectResponse = client.getObject(
            GetObjectRequest.builder()
                .bucket(bucket)
                .key(UPLOAD_FILE_NAME)
                .build()
        )
        assertThat(getObjectResponse.response().eTag()).isEqualTo(expectedEtag)
        assertThat(getObjectResponse.response().contentLength()).isEqualTo(uploadFile.length())
    }
}
