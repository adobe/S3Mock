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

package com.adobe.testing.s3mock.testsupport.common;

import static java.lang.String.join;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.S3MockApplication;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.net.Socket;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

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
   * Creates an {@link S3Client} client instance that is configured to call the started S3Mock
   * server using HTTPS.
   *
   * @return The {@link S3Client} instance.
   */
  public S3Client createS3ClientV2() {
    return S3Client.builder()
      .region(Region.of("us-east-1"))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
      .endpointOverride(URI.create(getServiceEndpoint()))
      .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(AttributeMap.builder()
        .put(TRUST_ALL_CERTIFICATES, Boolean.TRUE)
        .build()))
      .build();
  }

  /**
   * Creates an {@link AmazonS3} client instance that is configured to call the started S3Mock
   * server using HTTPS.
   *
   * @return The {@link AmazonS3} instance.
   */
  public AmazonS3 createS3Client() {
    return createS3Client("us-east-1");
  }

  /**
   * Creates an {@link AmazonS3} client instance that is configured to call the started S3Mock
   * server using HTTPS for a given region.
   *
   * @param region Region to define regional endpoint.
   *
   * @return The {@link AmazonS3} instance.
   */
  public AmazonS3 createS3Client(final String region) {
    final BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");

    return AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withClientConfiguration(
            configureClientToIgnoreInvalidSslCertificates(new ClientConfiguration()))
        .withEndpointConfiguration(getEndpointConfiguration(region))
        .enablePathStyleAccess()
        .build();
  }

  public int getPort() {
    return s3MockFileStore.getPort();
  }

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
   *
   * @return The adjusted instance.
   */
  public ClientConfiguration configureClientToIgnoreInvalidSslCertificates(
      final ClientConfiguration clientConfiguration) {

    clientConfiguration.getApacheHttpClientConfig()
        .withSslSocketFactory(new SSLConnectionSocketFactory(
            createBlindlyTrustingSslContext(),
            NoopHostnameVerifier.INSTANCE));

    return clientConfiguration;
  }

  protected EndpointConfiguration getEndpointConfiguration(final String region) {
    return new EndpointConfiguration(getServiceEndpoint(), region);
  }

  protected String getServiceEndpoint() {
    final boolean isSecureConnection = (boolean) properties.getOrDefault(
        S3MockApplication.PROP_SECURE_CONNECTION, true);
    return isSecureConnection ? "https://localhost:" + getPort()
        : "http://localhost:" + getHttpPort();
  }

  protected void start() {
    s3MockFileStore = S3MockApplication.start(properties);
  }

  protected void stop() {
    s3MockFileStore.stop();
  }

  private SSLContext createBlindlyTrustingSslContext() {
    try {
      final SSLContext sc = SSLContext.getInstance("TLS");

      sc.init(null, new TrustManager[]{new X509ExtendedTrustManager() {
        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
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
        public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
          // no-op
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
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

  public abstract static class BaseBuilder<T extends S3MockStarter> {

    protected final Map<String, Object> arguments = new HashMap<>();

    public BaseBuilder<T> withInitialBuckets(final String... initialBuckets) {
      arguments.put(S3MockApplication.PROP_INITIAL_BUCKETS, join(",", initialBuckets));
      return this;
    }

    public BaseBuilder<T> withHttpsPort(final int httpsPort) {
      arguments.put(S3MockApplication.PROP_HTTPS_PORT, String.valueOf(httpsPort));
      return this;
    }

    public BaseBuilder<T> withHttpPort(final int httpPort) {
      arguments.put(S3MockApplication.PROP_HTTP_PORT, String.valueOf(httpPort));
      return this;
    }

    public BaseBuilder<T> withRootFolder(final String rootFolder) {
      arguments.put(S3MockApplication.PROP_ROOT_DIRECTORY, rootFolder);
      return this;
    }

    public BaseBuilder<T> withSecureConnection(final boolean secureConnection) {
      arguments.put(S3MockApplication.PROP_SECURE_CONNECTION, secureConnection);
      return this;
    }

    /**
     * Reduces logging level WARN and suppresses the startup banner.
     *
     * @return the builder
     */
    public BaseBuilder<T> silent() {
      arguments.put(S3MockApplication.PROP_SILENT, true);
      return this;
    }

    /**
     * Creates the instance.
     *
     * @return The configured instance.
     */
    public abstract T build();
  }
}
