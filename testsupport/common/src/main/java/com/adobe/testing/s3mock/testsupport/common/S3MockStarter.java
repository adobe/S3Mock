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

package com.adobe.testing.s3mock.testsupport.common;

import static java.lang.String.join;

import com.adobe.testing.s3mock.S3MockApplication;
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
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

/**
 * Helps configuring and starting the S3Mock app and provides a configured client for it.
 */
public abstract class S3MockStarter {
  protected S3MockApplication s3MockFileStore;
  protected final Map<String, Object> properties;

  protected S3MockStarter(final Map<String, Object> properties) {
    this.properties = defaultProps();
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  protected Map<String, Object> defaultProps() {
    final Map<String, Object> args = new HashMap<>();
    args.put(S3MockApplication.PROP_HTTPS_PORT, "0");
    args.put(S3MockApplication.PROP_HTTP_PORT, "0");
    return args;
  }

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
   * @return The HTTPS port that the S3Mock uses.
   */
  public int getPort() {
    return s3MockFileStore.getPort();
  }

  /**
   * @return The HTTP port that the S3Mock uses.
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

  protected void start() {
    s3MockFileStore = S3MockApplication.start(properties);
  }

  protected void stop() {
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

  public static abstract class BaseBuilder<T extends S3MockStarter> {
    protected final Map<String, Object> arguments = new HashMap<>();

    /**
     * @param initialBuckets buckets that are to be created at startup
     * @return the builder
     */
    public BaseBuilder<T> withInitialBuckets(final String... initialBuckets) {
      arguments.put(S3MockApplication.PROP_INITIAL_BUCKETS, join(",", initialBuckets));
      return this;
    }

    /**
     * @param httpsPort The HTTPS port to use. If not configured, a random port will be used.
     * @return the builder
     */
    public BaseBuilder<T> withHttpsPort(final int httpsPort) {
      arguments.put(S3MockApplication.PROP_HTTPS_PORT, String.valueOf(httpsPort));
      return this;
    }

    /**
     * @param httpPort The HTTP port to use. If not configured, a random port will be used.
     * @return the builder
     */
    public BaseBuilder<T> withHttpPort(final int httpPort) {
      arguments.put(S3MockApplication.PROP_HTTP_PORT, String.valueOf(httpPort));
      return this;
    }

    /**
     * @param rootFolder The root directory to use. If not configured, a default temp-dir will be
     *                  used.
     * @return the builder
     */
    public BaseBuilder<T> withRootFolder(final String rootFolder) {
      arguments.put(S3MockApplication.PROP_ROOT_DIRECTORY, rootFolder);
      return this;
    }

    /**
     * Reduces logging level WARN and suppresses the startup banner.
     * @return the builder
     */
    public BaseBuilder<T> silent() {
      arguments.put(S3MockApplication.PROP_SILENT, true);
      return this;
    }

    /**
     * @return The configured instance.
     */
    public abstract T build();
  }
}
