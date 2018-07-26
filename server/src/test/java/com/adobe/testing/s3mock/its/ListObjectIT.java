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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import com.adobe.testing.s3mock.S3MockApplication;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class ListObjectIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ListObjectIT.class);

  private static final String BUCKET_NAME = "list-objects-test";

  private static final String[] ALL_OBJECTS = 
      new String[] {"a", "b", "b/1", "b/1/1", "b/1/2", "b/2", "c/1", "c/1/1", "d:1", "d:1:1"};
  
  static class Param {
    final String prefix;

    final String delimiter;

    String[] expectedKeys = new String[0];

    String[] expectedPrefixes= new String[0];

    private Param(String prefix, String delimiter) {
      this.prefix = prefix;
      this.delimiter = delimiter;
    }
    
    Param keys(String... expectedKeys) {
      this.expectedKeys = expectedKeys;
      return this;
    }
    
    Param prefixes(String... expectedPrefixes) {
      this.expectedPrefixes = expectedPrefixes;
      return this;
    }
    
    @Override
    public String toString() {
      return String.format("prefix=%s, delimiter=%s", prefix, delimiter);
    }
  }
  
  static Param param(String prefix, String delimiter) {
    return new Param(prefix, delimiter);
  }
  
  /**
   * Parameter fatcory.
   * 
   * @return
   */
  @Parameters(name = "{index}: {0}")
  public static Iterable<Param> data() {
    return Arrays.asList(//
        param(null, null).keys(ALL_OBJECTS), //
        param("", null).keys(ALL_OBJECTS), //
        param(null, "").keys(ALL_OBJECTS), //
        param("/", null), //
        param("b", null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b/", null).keys("b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b", "/").keys("b").prefixes("b/"), //
        param("b/", "/").keys("b/1", "b/2").prefixes("b/1/"), //
        param("b/1", "/").keys("b/1").prefixes("b/1/"), //
        param("b/1/", "/").keys("b/1/1", "b/1/2"), //
        param("c", "/").prefixes("c/"), //
        param("c/", "/").keys("c/1").prefixes("c/1/") //
    );
  }

  @Parameter(0)
  public Param parameters;


  private static S3MockApplication s3mock;

  private static AmazonS3 s3client;
  
  /**
   * Initialize the test bucket.
   */
  @BeforeClass
  public static void createBucket() {
    s3mock = S3MockApplication.start();

    s3client = createMockClient();
    
    try {
      s3client.deleteBucket(BUCKET_NAME);
    } catch (SdkClientException e) {
      // ignored
    }
    s3client.createBucket(BUCKET_NAME);

    // create all expected objects
    for (String key : ALL_OBJECTS) {
      s3client.putObject(BUCKET_NAME, key, "Test");
    }
  }

  private static AmazonS3 createMockClient() {
    final BasicAWSCredentials credentials = new BasicAWSCredentials("foo", "bar");

    return AmazonS3ClientBuilder.standard() //
        .withCredentials(new AWSStaticCredentialsProvider(credentials)) //
        .withClientConfiguration(new ClientConfiguration()) //
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
            "http://localhost:" + s3mock.getHttpPort(), "us-east-1")) //
        .enablePathStyleAccess() //
        .build();
  }

  @Test
  public void listV1() {
    ObjectListing l = s3client.listObjects(
        new ListObjectsRequest(BUCKET_NAME, parameters.prefix, null, parameters.delimiter, null));

    LOGGER.info(
        "list V1, prefix='{}', delimiter='{}': \n  Objects: \n    {}\n  Prefixes: \n    {}\n", //
        parameters.prefix, //
        parameters.delimiter, //
        l.getObjectSummaries().stream().map(s -> s.getKey()).collect(joining("\n    ")), //
        l.getCommonPrefixes().stream().collect(joining("\n    ")) //
    );

    assertThat("Returned keys are correct",
        l.getObjectSummaries().stream().map(s -> s.getKey()).collect(toList()),
        parameters.expectedKeys.length > 0 ? contains(parameters.expectedKeys) : empty());
    assertThat("Returned prefixes are correct", l.getCommonPrefixes().stream().collect(toList()),
        parameters.expectedPrefixes.length > 0 ? contains(parameters.expectedPrefixes) : empty());
  }
}
