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

import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.jetty.util.UrlEncoded;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * Verifies raw HTTP results for those methods where S3 Client from AWS SDK does not return anything
 * resp. where its not possible to verify e.g. status codes.
 */
public class PlainHttpIT extends S3TestBase {

  private static final String SLASH = "/";

  private CloseableHttpClient httpClient;

  @BeforeEach
  public void setupHttpClient() {
    httpClient = HttpClients.createDefault();
  }

  @AfterEach
  public void shutdownHttpClient() throws IOException {
    httpClient.close();
  }

  @Test
  public void putObjectReturns200() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());
    final HttpPut putObject = new HttpPut(SLASH + targetBucket.getName());
    putObject.setEntity(new ByteArrayEntity(UUID.randomUUID().toString().getBytes()));

    final HttpResponse putObjectResponse =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), putObject);
    assertThat(putObjectResponse.getStatusLine().getStatusCode(), is(SC_OK));
  }

  @Test
  public void putObjectEncryptedWithAbsentKeyRef() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());
    final HttpPut putObject = new HttpPut(SLASH + targetBucket.getName());
    putObject.addHeader("x-amz-server-side-encryption", "aws:kms");
    putObject.setEntity(new ByteArrayEntity(UUID.randomUUID().toString().getBytes()));

    final HttpResponse putObjectResponse =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), putObject);
    assertThat(putObjectResponse.getStatusLine().getStatusCode(), is(SC_OK));
  }

  @Test
  public void listWithPrefixAndMissingSlash() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());
    s3Client.putObject(targetBucket.getName(), "prefix", "Test");

    final HttpGet getObject = new HttpGet(SLASH + targetBucket.getName()
        + "?prefix=prefix%2F&encoding-type=url");

    final HttpResponse getObjectResponse =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), getObject);
    assertThat(getObjectResponse.getStatusLine().getStatusCode(), is(SC_OK));
  }

  @Test
  public void getObjectUsesApplicationXmlContentType() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());

    final HttpGet getObject = new HttpGet(SLASH + targetBucket.getName());
    assertApplicationXmlContentType(getObject);
  }

  @Test
  public void listBucketsUsesApplicationXmlContentType() throws IOException {
    s3Client.createBucket(UUID.randomUUID().toString());

    final HttpGet listBuckets = new HttpGet(SLASH);
    assertApplicationXmlContentType(listBuckets);
  }

  @Test
  public void batchDeleteUsesApplicationXmlContentType() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());

    final HttpPost postObject = new HttpPost(SLASH + targetBucket.getName() + "?delete");
    postObject.setEntity(new StringEntity(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>"
                    + "<Object><Key>myFile-1</Key></Object>"
                    + "<Object><Key>myFile-2</Key></Object>"
                    + "</Delete>", (ContentType) null));
    assertApplicationXmlContentType(postObject);
  }

  @Test
  public void putObjectWithSpecialCharactersInTheName() throws Exception {
    final String fileNameWithSpecialCharacters = "file=name$Dollar;Semicolon"
            + "&Ampersand@At:Colon     Space,Comma?Questionmark";
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());
    final HttpPut putObject = new HttpPut(SLASH + targetBucket.getName()
            + SLASH + UrlEncoded.encodeString(fileNameWithSpecialCharacters));
    putObject.setEntity(new ByteArrayEntity(UUID.randomUUID().toString().getBytes()));

    final HttpResponse putObjectResponse =
            httpClient.execute(new HttpHost(getHost(), getHttpPort()), putObject);

    assertThat(putObjectResponse.getStatusLine().getStatusCode(), is(SC_OK));
    assertThat(s3Client
                    .listObjects(targetBucket.getName())
                    .getObjectSummaries()
                    .get(0)
                    .getKey(),
            is(fileNameWithSpecialCharacters));
  }

  @Test
  public void deleteNonExistingObjectReturns204() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());
    final HttpDelete deleteObject =
        new HttpDelete(SLASH + targetBucket.getName() + SLASH + UUID.randomUUID().toString());

    final HttpResponse deleteObjectResponse =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), deleteObject);
    assertThat(deleteObjectResponse.getStatusLine().getStatusCode(), is(SC_NO_CONTENT));
  }

  @Test
  public void batchDeleteObjects() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());

    final HttpPost postObject = new HttpPost(SLASH + targetBucket.getName() + "?delete");
    postObject.setEntity(new StringEntity("<?xml version=\"1.0\" "
        + "encoding=\"UTF-8\"?><Delete><Object><Key>myFile-1</Key></Object><Object><Key>myFile-2"
        + "</Key></Object></Delete>", (ContentType) null));

    final CloseableHttpResponse response =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), postObject);

    assertThat(response.getStatusLine().getStatusCode(), is(SC_OK));
  }

  @Test
  public void headObjectWithUnknownContentType() throws IOException {
    final Bucket targetBucket = s3Client.createBucket(UUID.randomUUID().toString());

    final byte[] contentAsBytes = new byte[0];
    final ObjectMetadata md = new ObjectMetadata();
    md.setContentLength(contentAsBytes.length);
    md.setContentType(UUID.randomUUID().toString());
    final String blankContentTypeFilename = UUID.randomUUID().toString();
    s3Client.putObject(targetBucket.getName(), blankContentTypeFilename,
        new ByteArrayInputStream(contentAsBytes), md);

    final HttpHead headObject =
        new HttpHead(SLASH + targetBucket.getName() + SLASH + blankContentTypeFilename);

    final HttpResponse headObjectResponse =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), headObject);

    assertThat(headObjectResponse.getStatusLine().getStatusCode(), is(SC_OK));
  }

  private void assertApplicationXmlContentType(final HttpRequestBase httpRequestBase)
          throws IOException {
    final HttpResponse response =
        httpClient.execute(new HttpHost(getHost(), getHttpPort()), httpRequestBase);
    assertThat(
        response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue(),
        is("application/xml;charset=UTF-8")
    );
  }
}
