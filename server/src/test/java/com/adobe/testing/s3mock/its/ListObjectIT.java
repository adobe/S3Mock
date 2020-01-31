/*
 *  Copyright 2017-2020 Adobe.
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

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListObjectIT extends S3TestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ListObjectIT.class);

  private static final String BUCKET_NAME = "list-objects-test";

  private static final String[] ALL_OBJECTS =
      new String[]{"3330/0", "33309/0", "a",
          "b", "b/1", "b/1/1", "b/1/2", "b/2",
          "c/1", "c/1/1",
          "d:1", "d:1:1",
          "eor.txt", "foo/eor.txt"};

  static class Param {

    final String prefix;

    final String delimiter;

    final String startAfter;

    String[] expectedKeys = new String[0];

    String[] expectedPrefixes = new String[0];

    private Param(final String prefix, final String delimiter, final String startAfter) {
      this.prefix = prefix;
      this.delimiter = delimiter;
      this.startAfter = startAfter;
    }

    Param keys(final String... expectedKeys) {
      this.expectedKeys = expectedKeys;
      return this;
    }

    Param prefixes(final String... expectedPrefixes) {
      this.expectedPrefixes = expectedPrefixes;
      return this;
    }

    @Override
    public String toString() {
      return String.format("prefix=%s, delimiter=%s, startAfter=%s", prefix, delimiter, startAfter);
    }
  }

  static Param param(final String prefix, final String delimiter, final String startAfter) {
    return new Param(prefix, delimiter, startAfter);
  }

  /**
   * Parameter factory.
   */
  public static Iterable<Param> data() {
    return Arrays.asList(//
        param(null, null, null).keys(ALL_OBJECTS), //
        param("", null, null).keys(ALL_OBJECTS), //
        param(null, "", null).keys(ALL_OBJECTS), //
        param("/", null, null), //
        param("b", null, null).keys("b", "b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b/", null, null).keys("b/1", "b/1/1", "b/1/2", "b/2"), //
        param("b", "/", null).keys("b").prefixes("b/"), //
        param("b/", "/", null).keys("b/1", "b/2").prefixes("b/1/"), //
        param("b/1", "/", null).keys("b/1").prefixes("b/1/"), //
        param("b/1/", "/", null).keys("b/1/1", "b/1/2"), //
        param("c", "/", null).prefixes("c/"), //
        param("c/", "/", null).keys("c/1").prefixes("c/1/"), //
        param("eor", "/", null).keys("eor.txt"), //
        // start after existing key
        param("b", null, "b/1/1").keys("b/1/2", "b/2"), //
        // start after non-existing key
        param("b", null, "b/0").keys("b/1", "b/1/1", "b/1/2", "b/2"),
        param("3330/", null, null).keys("3330/0"),
        param(null, null, "!%-_.*,()").keys(ALL_OBJECTS)
    );
  }

  /**
   * Initialize the test bucket.
   */
  @BeforeEach
  public void initializeTestBucket() {
    // I'm not sure why this is needed.
    // It seems like @RunWith(Parameterized) breaks the parent
    // life cycle method invocation
    super.prepareS3Client();

    s3Client.createBucket(BUCKET_NAME);

    // create all expected objects
    for (final String key : ALL_OBJECTS) {
      s3Client.putObject(BUCKET_NAME, key, "Test");
    }
  }

  /**
   * Test the list V1 endpoint.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void listV1(final Param parameters) {
    final ObjectListing l = s3Client.listObjects(
        new ListObjectsRequest(BUCKET_NAME, parameters.prefix, parameters.startAfter,
            parameters.delimiter, null));

    LOGGER.info(
        "list V1, prefix='{}', delimiter='{}': \n  Objects: \n    {}\n  Prefixes: \n    {}\n", //
        parameters.prefix, //
        parameters.delimiter, //
        l.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(joining("\n    ")), //
        String.join("\n    ", l.getCommonPrefixes()) //
    );

    assertThat("Returned keys are correct",
        l.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(toList()),
        parameters.expectedKeys.length > 0 ? contains(parameters.expectedKeys) : empty());
    assertThat("Returned prefixes are correct", new ArrayList<>(l.getCommonPrefixes()),
        parameters.expectedPrefixes.length > 0 ? contains(parameters.expectedPrefixes) : empty());
  }

  /**
   * Test the list V2 endpoint.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void listV2(final Param parameters) {
    final ListObjectsV2Result l = s3Client.listObjectsV2(new ListObjectsV2Request()
        .withBucketName(BUCKET_NAME)
        .withDelimiter(parameters.delimiter)
        .withPrefix(parameters.prefix)
        .withStartAfter(parameters.startAfter));

    LOGGER.info(
        "list V1, prefix='{}', delimiter='{}', startAfter='{}': "
            + "\n  Objects: \n    {}\n  Prefixes: \n    {}\n", //
        parameters.prefix, //
        parameters.delimiter, //
        parameters.startAfter, //
        l.getObjectSummaries().stream().map(s -> URLDecoder.decode(s.getKey()))
            .collect(joining("\n    ")), //
            String.join("\n    ", l.getCommonPrefixes()) //
    );

    assertThat("Returned keys are correct",
        l.getObjectSummaries().stream().map(s -> URLDecoder.decode(s.getKey())).collect(toList()),
        parameters.expectedKeys.length > 0 ? contains(parameters.expectedKeys) : empty());
    assertThat("Returned prefixes are correct", l.getCommonPrefixes().stream().collect(toList()),
        parameters.expectedPrefixes.length > 0 ? contains(parameters.expectedPrefixes) : empty());
  }
}
