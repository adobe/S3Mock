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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.util.Strings;
import org.junit.AfterClass;
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
      arr("a", "b", "b/1", "b/1/1", "b/1/2", "b/2", "c/1", "c/1/1");

  private static final boolean RUN_AGAINST_AWS = false;
  
  private static <T> T[] arr(@SuppressWarnings("unchecked") final T... a) {
    return a;
  }

  /**
   * Parameter fatcory.
   * 
   * @return
   */
  @Parameters(name = "{index}: prefix={0}, delimiter={1}")
  public static Iterable<Object[]> data() {
    return Arrays.<Object[]>asList(//
        arr(null, null, ALL_OBJECTS, arr()), //
        arr("", null, ALL_OBJECTS, arr()), //
        arr(null, "", ALL_OBJECTS, arr()), //
        arr("/", null, arr(), arr()), //
        arr("b", null, arr("b", "b/1", "b/1/1", "b/1/2", "b/2"), arr()), //
        arr("b/", null, arr("b/1", "b/1/1", "b/1/2", "b/2"), arr()), //
        arr("b", "/", arr("b"), arr("b/")), //
        arr("b/", "/", arr("b/1", "b/2"), arr("b/1/")), //
        arr("b/1", "/", arr("b/1"), arr("b/1/")), //
        arr("b/1/", "/", arr("b/1/1", "b/1/2"), arr()), //
        arr("c", "/", arr(), arr("c/")), //
        arr("c/", "/", arr("c/1"), arr("c/1/")) //
    );
  }

  private static final List<String> param = new ArrayList<>();

  @Parameter(0)
  public String prefix;

  @Parameter(1)
  public String delimiter;

  @Parameter(2)
  public Object[] expectedKeys;

  @Parameter(3)
  public Object[] expectedPrefixes;


  private static S3MockApplication s3mock;

  private static AmazonS3 s3client;
  
  /**
   * Initialize the test bucket.
   */
  @BeforeClass
  public static void createBucket() {
    s3mock = S3MockApplication.start();

    s3client = RUN_AGAINST_AWS ? createAwsClient() : createMockClient();
    
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

  private static AmazonS3 createAwsClient() {
    AWSCredentials credentials = new BasicAWSCredentials("key", "secret");

    return AmazonS3ClientBuilder.standard() //
        .withCredentials(new AWSStaticCredentialsProvider(credentials)) //
        .withRegion(Regions.DEFAULT_REGION) //
        .build();
  }

  @AfterClass
  public static void logParams() {
    LOGGER.info("Expected parameters are: \n{} //\n", param.stream().collect(joining(", //\n")));
  }

  @Test
  public void listV1() {
    ObjectListing l = s3client.listObjects(
        new ListObjectsRequest(BUCKET_NAME, prefix, null, delimiter, null));

    LOGGER.info(
        "list V1, prefix='{}', delimiter='{}': \n  Objects: \n    {}\n  Prefixes: \n    {}\n", //
        prefix, //
        delimiter, //
        l.getObjectSummaries().stream().map(s -> s.getKey()).collect(joining("\n    ")), //
        l.getCommonPrefixes().stream().collect(joining("\n    ")) //
    );

    param.add(String.format("arr(%s, %s, arr(%s), arr(%s))", //
        qq(prefix), //
        qq(delimiter), //
        l.getObjectSummaries().isEmpty()
            ? ""
            : l.getObjectSummaries().stream().map(s -> s.getKey()).collect(
                joining("\", \"", "\"", "\"")), //
        l.getCommonPrefixes().isEmpty()
            ? ""
            : l.getCommonPrefixes().stream().collect(joining("\", \"", "\"", "\""))));

    assertThat("Returned keys are correct",
        l.getObjectSummaries().stream().map(s -> s.getKey()).collect(toList()),
        expectedKeys.length > 0 ? contains(expectedKeys) : empty());
    assertThat("Returned prefixes are correct", l.getCommonPrefixes().stream().collect(toList()),
        expectedPrefixes.length > 0 ? contains(expectedPrefixes) : empty());
  }

  private String qq(final String s) {
    return null != s ? Strings.dquote(s) : "null";
  }
}
