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
import java.io.IOException;
import java.util.UUID;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
}
