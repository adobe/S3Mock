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

import static java.util.Arrays.asList;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

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
import java.net.URI;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Base type for S3 Mock integration tests. Sets up S3 Client, Certificates, initial Buckets, etc.
 */
abstract class S3TestBase {

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
  S3Client s3ClientV2;

  /**
   * Configures the S3-Client to be used in the Test. Sets the SSL context to accept untrusted SSL
   * connections.
   */
  @BeforeEach
  public void prepareS3Client() {
    s3Client = defaultTestAmazonS3ClientBuilder().build();
    s3ClientV2 = createS3ClientV2();
  }

  protected AmazonS3ClientBuilder defaultTestAmazonS3ClientBuilder() {
    return AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")))
        .withClientConfiguration(ignoringInvalidSslCertificates(new ClientConfiguration()))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(getServiceEndpoint(), "us-east-1"))
        .enablePathStyleAccess();
  }

  private String getServiceEndpoint() {
    return "https://" + getHost() + ":" + getPort();
  }

  public S3Client createS3ClientV2() {
    return S3Client.builder()
                   .region(Region.of("us-east-1"))
                   .credentialsProvider(
                       StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
                   .endpointOverride(URI.create(getServiceEndpoint()))
                   .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(
                       AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
                   .build();
  }

  /**
   * Deletes all existing buckets.
   */
  @AfterEach
  public void cleanupFilestore() {
    for (final Bucket bucket : s3Client.listBuckets()) {
      if (!INITIAL_BUCKET_NAMES.contains(bucket.getName())) {
        s3Client.listMultipartUploads(new ListMultipartUploadsRequest(bucket.getName()))
            .getMultipartUploads().forEach(upload -> s3Client.abortMultipartUpload(
            new AbortMultipartUploadRequest(bucket.getName(), upload.getKey(),
                upload.getUploadId())));
        s3Client.listObjects(bucket.getName()).getObjectSummaries().forEach(
            (object -> s3Client.deleteObject(bucket.getName(), object.getKey())));
        s3Client.deleteBucket(bucket.getName());
      }
    }
  }

  String getHost() {
    return System.getProperty("it.s3mock.host", "localhost");
  }

  private int getPort() {
    return Integer.getInteger("it.s3mock.port_https", 9191);
  }

  int getHttpPort() {
    return Integer.getInteger("it.s3mock.port_http", 9090);
  }

  private ClientConfiguration ignoringInvalidSslCertificates(
      final ClientConfiguration clientConfiguration) {

    clientConfiguration.getApacheHttpClientConfig()
        .withSslSocketFactory(new SSLConnectionSocketFactory(
            createBlindlyTrustingSslContext(),
            NoopHostnameVerifier.INSTANCE));

    return clientConfiguration;
  }

  private SSLContext createBlindlyTrustingSslContext() {
    try {
      final SSLContext sc = SSLContext.getInstance("TLS");

      sc.init(null, new TrustManager[] {new X509ExtendedTrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2) {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
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
