/*
 *  Copyright 2017-2018 Adobe.
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

import static java.util.Arrays.asList;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.After;
import org.junit.Before;

/**
 * Base type for S3 Mock integration tests. Sets up S3 Client, Certificates, initial Buckets, etc.
 */
public abstract class S3TestBase {
  static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket-a", "bucket-b");

  static final String TEST_ENC_KEYREF =
      "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-ref";

  static final String BUCKET_NAME = "mydemotestbucket";

  static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";

  static final String TEST_WRONG_KEYREF =
      "arn:aws:kms:us-east-1:1234567890:keyWRONGWRONGWRONG/2d70f7f6-b484-4309-91d5-7813b7dd46ce";
  static final int _1MB = 1024 * 1024;
  static final long _2MB = 2L * _1MB;
  private static final long _6BYTE = 6L;

  private static final int THREAD_COUNT = 50;

  AmazonS3 s3Client;

  /**
   * Configures the S3-Client to be used in the Test. Sets the SSL context to accept untrusted SSL
   * connections.
   */
  @Before
  public void prepareS3Client() {
    final BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");

    s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withClientConfiguration(ignoringInvalidSslCertificates(new ClientConfiguration()))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration("https://" + getHost() + ":" + getPort(),
                "us-east-1"))
        .enablePathStyleAccess()
        .build();
  }

  /**
   * Deletes all existing buckets
   */
  @After
  public void cleanupFilestore() {
    for (final Bucket bucket : s3Client.listBuckets()) {
      if (!INITIAL_BUCKET_NAMES.contains(bucket.getName())) {
        s3Client.listMultipartUploads(new ListMultipartUploadsRequest(bucket.getName()))
            .getMultipartUploads()
            .forEach(upload ->
                s3Client.abortMultipartUpload(
                    new AbortMultipartUploadRequest(bucket.getName(), upload.getKey(),
                        upload.getUploadId()))
            );
        s3Client.deleteBucket(bucket.getName());
      }
    }
  }

  private String getHost() {
    return System.getProperty("it.s3mock.host", "localhost");
  }

  private int getPort() {
    return Integer.getInteger("it.s3mock.port_https", 9191);
  }

  private ClientConfiguration ignoringInvalidSslCertificates(
      final ClientConfiguration clientConfiguration) {

    clientConfiguration.getApacheHttpClientConfig()
        .withSslSocketFactory(new SSLConnectionSocketFactory(
            createBlindlyTrustingSSLContext(),
            NoopHostnameVerifier.INSTANCE));

    return clientConfiguration;
  }

  private SSLContext createBlindlyTrustingSSLContext() {
    try {
      final SSLContext sc = SSLContext.getInstance("TLS");

      sc.init(null, new TrustManager[] {new X509ExtendedTrustManager() {
        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2) {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2) {
          // no-op
        }
      }
      }, new java.security.SecureRandom());

      return sc;
    } catch (final NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }

  TransferManager createDefaultTransferManager() {
    return createTransferManager(_6BYTE, _6BYTE, _6BYTE, _6BYTE);
  }

  TransferManager createTransferManager(final long multipartUploadThreshold,
      final long multipartUploadPartSize,
      final long multipartCopyThreshold,
      final long multipartCopyPartSize) {
    final ThreadFactory threadFactory = new ThreadFactory() {
      private int threadCount = 1;

      @Override
      public Thread newThread(final Runnable r) {
        final Thread thread = new Thread(r);
        thread.setName("s3-transfer-" + threadCount++);

        return thread;
      }
    };

    return TransferManagerBuilder.standard()
        .withS3Client(s3Client)
        .withExecutorFactory(() -> Executors.newFixedThreadPool(THREAD_COUNT, threadFactory))
        .withMultipartUploadThreshold(multipartUploadThreshold)
        .withMinimumUploadPartSize(multipartUploadPartSize)
        .withMultipartCopyPartSize(multipartCopyPartSize)
        .withMultipartCopyThreshold(multipartCopyThreshold)
        .build();
  }


  InputStream randomInputStream(final int size) {
    final byte[] content = new byte[size];
    new Random().nextBytes(content);

    return new ByteArrayInputStream(content);
  }
}
