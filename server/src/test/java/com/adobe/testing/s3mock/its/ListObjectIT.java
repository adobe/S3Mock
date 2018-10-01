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

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListObjectIT extends S3TestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ListObjectIT.class);

  private static final String BUCKET_NAME = "list-objects-test";

  private static final String[] ALL_OBJECTS = 
      new String[] {"a",
          "b", "b/1", "b/1/1", "b/1/2", "b/2",
          "c/1", "c/1/1",
          "d:1", "d:1:1",
          "eor.txt", "foo/eor.txt"};
  
  static class Param {
    final String prefix;

    final String delimiter;

    String[] expectedKeys = new String[0];

    String[] expectedPrefixes = new String[0];

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
   * Parameter factory.
   * 
   * @return
   */
  @ParameterizedTest(name = "{index}: {0}")
  @ValueSource(classes = {Param.class})
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
        param("c/", "/").keys("c/1").prefixes("c/1/"), //
        param("eor", "/").keys("eor.txt") //
    );
  }

  public Param parameters;

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
    for (String key : ALL_OBJECTS) {
      s3Client.putObject(BUCKET_NAME, key, "Test");
    }
  }

  @Test
  public void listV1() {
    ObjectListing l = s3Client.listObjects(
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
