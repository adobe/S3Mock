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

package com.adobe.testing.s3mock

import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm
import software.amazon.awssdk.checksums.spi.ChecksumAlgorithm
import software.amazon.awssdk.http.auth.aws.internal.signer.CredentialScope
import software.amazon.awssdk.http.auth.aws.internal.signer.RollingSigner
import software.amazon.awssdk.http.auth.aws.internal.signer.chunkedencoding.ChecksumTrailerProvider
import software.amazon.awssdk.http.auth.aws.internal.signer.chunkedencoding.ChunkedEncodedInputStream
import software.amazon.awssdk.http.auth.aws.internal.signer.chunkedencoding.SigV4ChunkExtensionProvider
import software.amazon.awssdk.http.auth.aws.internal.signer.chunkedencoding.SigV4TrailerProvider
import software.amazon.awssdk.http.auth.aws.internal.signer.chunkedencoding.TrailerProvider
import software.amazon.awssdk.http.auth.aws.internal.signer.io.ChecksumInputStream
import software.amazon.awssdk.http.auth.aws.internal.signer.util.ChecksumUtil
import software.amazon.awssdk.http.auth.aws.internal.signer.util.SignerUtils
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.time.Instant
import java.util.stream.Stream

object ChecksumTestUtil {

  fun prepareInputStream(
      input: File,
      signed: Boolean = true,
      algorithm: ChecksumAlgorithm? = null,
  ): Pair<InputStream, Long> {
    val builder = ChunkedEncodedInputStream.builder().apply {
      inputStream(Files.newInputStream(input.toPath()))
      algorithm?.let { setupChecksumTrailer(this, it) }
      if (signed) {
        setupSignedTrailerAndExtension(this)
      }
    }

    val chunkedEncodingInputStream: InputStream = builder
      .chunkSize(4000)
      .build()

    val decodedLength = input.length()

    return chunkedEncodingInputStream to decodedLength
  }

  fun setupSignedTrailerAndExtension(builder: ChunkedEncodedInputStream.Builder) {
    val seedSignature = "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"
    val credentialScope =
        CredentialScope("us-east-1", "s3", Instant.parse("2013-05-24T00:00:00Z"))
    val credentials =
      AwsCredentialsIdentity.create("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    val signingKey = SignerUtils.deriveSigningKey(credentials, credentialScope)
    val rollingSigner = RollingSigner(signingKey, seedSignature)
    val sigV4ChunkExtensionProvider = SigV4ChunkExtensionProvider(rollingSigner, credentialScope)
    val sigV4TrailerProvider = SigV4TrailerProvider(emptyList(), rollingSigner, credentialScope)

    builder.addTrailer(sigV4TrailerProvider)
    builder.addExtension(sigV4ChunkExtensionProvider)
  }

  fun setupChecksumTrailer(
      builder: ChunkedEncodedInputStream.Builder,
      checksumAlgorithm: ChecksumAlgorithm
  ) {
    val checksumHeaderName = ChecksumUtil.checksumHeaderName(checksumAlgorithm)
    val sdkChecksum = ChecksumUtil.fromChecksumAlgorithm(checksumAlgorithm)
    val checksumInputStream = ChecksumInputStream(
        builder.inputStream(),
        mutableSetOf(sdkChecksum)
    )

    val checksumTrailer: TrailerProvider = ChecksumTrailerProvider(sdkChecksum, checksumHeaderName)

    builder.inputStream(checksumInputStream).addTrailer(checksumTrailer)
  }

  @JvmStatic
  fun algorithms(): Stream<ChecksumAlgorithm> = Stream.of(
      DefaultChecksumAlgorithm.SHA256,
      DefaultChecksumAlgorithm.SHA1,
      DefaultChecksumAlgorithm.CRC32,
      DefaultChecksumAlgorithm.CRC32C,
      DefaultChecksumAlgorithm.CRC64NVME
  )
}
