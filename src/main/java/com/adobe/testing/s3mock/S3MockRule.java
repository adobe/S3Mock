/*
 *  Copyright 2017 Adobe.
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

/**
 * JUnit rule to start and stop the S3 Mock Application. After the
 * tests, the S3 Mock is stopped and the certificate is removed. It should be used as
 * {@link ClassRule}:
 *
 * <pre>
 * &#64;ClassRule
 * public static S3MockRule S3_MOCK_RULE = new S3MockRule();
 *
 * private final AmazonS3 s3Client = S3_MOCK_RULE.createS3Client();
 *
 * &#64;Test
 * public void doSomethingWithS3() {
 *   s3Client.createBucket("myBucket");
 * }
 * </pre>
 */
public class S3MockRule extends ExternalResource {
  private S3MockApplication s3MockFileStore;

  /**
   * @return An {@link AmazonS3} client instance that is configured to call the started S3Mock
   *         server using HTTPS.
   */
  public AmazonS3 createS3Client() {
    final BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");

    return AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withClientConfiguration(
            configureClientToIgnoreInvalidSslCertificates(new ClientConfiguration()))
        .withEndpointConfiguration(
            new EndpointConfiguration("https://localhost:" + getPort(), "us-east-1"))
        .enablePathStyleAccess()
        .build();
  }

  /**
   * Returns the HTTPS port that the S3 Mock uses.
   *
   * @return The HTTPS port that the S3 Mock uses.
   */
  public int getPort() {
    return s3MockFileStore.getPort();
  }

  /**
   * Returns the HTTP port that the S3 Mock uses.
   *
   * @return The HTTP port that the S3 Mock uses.
   */
  public int getHttpPort() {
    return s3MockFileStore.getHttpPort();
  }

  /**
   * Registers a valid KMS key reference in the mock server.
   *
   * @param keyRef A KMS Key Reference
   */
  public void registerKMSKeyRef(final String keyRef) {
    s3MockFileStore.registerKMSKeyRef(keyRef);
  }

  /**
   * Adjusts the given client configuration to allow the communication with the mock server using
   * HTTPS, although that one uses a self-signed SSL certificate.
   *
   * @param clientConfiguration The {@link ClientConfiguration} to adjust.
   * @return The adjusted instance.
   */
  public ClientConfiguration configureClientToIgnoreInvalidSslCertificates(
      final ClientConfiguration clientConfiguration) {

    clientConfiguration.getApacheHttpClientConfig()
        .withSslSocketFactory(new SSLConnectionSocketFactory(
            createBlindlyTrustingSSLContext(),
            NoopHostnameVerifier.INSTANCE));

    return clientConfiguration;
  }

  @Override
  protected void before() {
    s3MockFileStore = S3MockApplication.start();
  }

  @Override
  protected void after() {
    s3MockFileStore.stop();
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
            final Socket arg2)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkClientTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final Socket arg2)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] arg0, final String arg1,
            final SSLEngine arg2)
            throws CertificateException {
          // no-op
        }
      }
      }, new java.security.SecureRandom());

      return sc;
    } catch (final NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }
}
