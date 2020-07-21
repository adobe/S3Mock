/*
 *  Copyright 2020 Adobe.
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

package com.adobe.testing.s3mock.junit5.sdk1;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.InputStream;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Test ensures there's a way to configure user-provided certificate for S3Mock.
 *
 * <p>The certificate was created with {@code keytool -genkey -keyalg RSA -alias customcert
 * -keystore customcert.jks -storepass qwerty -keysize 2048 -ext san=dns:localhost}</p>
 */
@Execution(ExecutionMode.SAME_THREAD)
public class CustomCertificateTest {
  private static final String KEYSTORE_FILE_NAME = "customcert.jks";
  private static final String KEYSTORE_PASSWORD = "qwerty";
  private static final String KEY_ALIAS = "customcert";

  @RegisterExtension
  public static final S3MockExtension s3mock =
      S3MockExtension.builder()
          .withSslParameters("classpath:" + KEYSTORE_FILE_NAME, KEYSTORE_PASSWORD, KEY_ALIAS,
              KEYSTORE_PASSWORD)
          .build();

  @Test
  void connectWithCustomSSLContext() throws Exception {
    // We use regular Amazon S3 API to ensure it would be able to connect to S3 mock server
    // with no hacks like "allow any certificate"

    // Note: we still have to configure ClientConfiguration as there's no reliable way
    // to adjust the system-default TrustManager in the runtime.
    // An alternative approach is to use javax.net.ssl.truststore properties
    // at the Java process startup.

    AmazonS3 s3 = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials("foo", "bar")))
        .withClientConfiguration(createClientConfiguration(KEYSTORE_FILE_NAME))
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                "https://localhost:" + s3mock.getPort(),
                "us-west-1"))
        .enablePathStyleAccess()
        .build();

    // Below is a smoke-test of the API. The point is to check if SSL connectivity works
    String bucketName = "non-existent-bucket-to-verify-if-api-works";
    Assertions.assertFalse(s3.doesBucketExistV2(bucketName),
        () -> "Bucket " + bucketName + " must not be present at the mock server");

    s3.shutdown();
  }

  private static ClientConfiguration createClientConfiguration(String keystoreFileName)
      throws Exception {
    // It configures Apache Http Client to use our own trust store (==trusted certificate)
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.getApacheHttpClientConfig()
        .setSslSocketFactory(new SSLConnectionSocketFactory(createSslContext(keystoreFileName)));
    return clientConfiguration;
  }

  /**
   * Load a certificate from a given keystore and generate a {@code SSLContext} that trusts the
   * certificate.
   *
   * @param keystoreFileName keystore name to use
   * @return SSLContext
   * @throws Exception in case something fails
   */
  private static SSLContext createSslContext(String keystoreFileName) throws Exception {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    try (InputStream jks =
             CustomCertificateTest.class.getResourceAsStream("/" + keystoreFileName)) {
      ks.load(jks, KEYSTORE_PASSWORD.toCharArray());
    }
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);
    return sslContext;
  }
}
