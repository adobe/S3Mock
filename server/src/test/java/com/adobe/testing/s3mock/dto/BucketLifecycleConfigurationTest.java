/*
 *  Copyright 2017-2023 Adobe.
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

package com.adobe.testing.s3mock.dto;

import static com.adobe.testing.s3mock.dto.DtoTestUtil.deserialize;
import static com.adobe.testing.s3mock.dto.DtoTestUtil.serializeAndAssert;
import static com.adobe.testing.s3mock.dto.LifecycleRule.Status.ENABLED;
import static com.adobe.testing.s3mock.dto.StorageClass.GLACIER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class BucketLifecycleConfigurationTest {

  @Test
  void testDeserialization(TestInfo testInfo) throws IOException {
    var iut = deserialize(BucketLifecycleConfiguration.class, testInfo);

    var rules = iut.rules();
    assertThat(rules).hasSize(2);
    var rule1 = rules.get(0);
    assertThat(rule1.id()).isEqualTo("id1");
    assertThat(rule1.expiration()).isNull();
    var filter1 = rule1.filter();
    assertThat(filter1).isNotNull();
    assertThat(filter1.prefix()).isEqualTo("documents/");
    assertThat(rule1.status()).isEqualTo(ENABLED);
    var transitions1 = rule1.transitions();
    assertThat(transitions1).hasSize(1);
    assertThat(transitions1.get(0).date()).isNull();
    assertThat(transitions1.get(0).days()).isEqualTo(30);
    assertThat(transitions1.get(0).storageClass()).isEqualTo(GLACIER);
    assertThat(rule1.abortIncompleteMultipartUpload()).isNull();
    assertThat(rule1.noncurrentVersionExpiration()).isNull();
    assertThat(rule1.noncurrentVersionTransitions()).isNull();

    var rule2 = rules.get(1);
    assertThat(rule2.id()).isEqualTo("id2");
    var filter2 = rule2.filter();
    assertThat(filter2).isNotNull();
    assertThat(filter2.prefix()).isEqualTo("logs/");
    assertThat(rule2.status()).isEqualTo(ENABLED);
    assertThat(rule2.expiration()).isNotNull();
    assertThat(rule2.expiration().days()).isEqualTo(365);
    assertThat(rule2.abortIncompleteMultipartUpload()).isNull();
    assertThat(rule2.noncurrentVersionExpiration()).isNull();
    assertThat(rule2.noncurrentVersionTransitions()).isNull();

  }

  @Test
  void testSerialization(TestInfo testInfo) throws IOException {
    var filter1 = new LifecycleRuleFilter(null, null, "documents/", null, null);
    var transition1 = new Transition(null, 30, GLACIER);
    var rule1 = new LifecycleRule(null, null, filter1, "id1", null, null,
        ENABLED, Collections.singletonList(transition1));
    var filter2 = new LifecycleRuleFilter(null, null, "logs/", null, null);
    var expiration2 = new LifecycleExpiration(null, 365, null);
    var rule2 = new LifecycleRule(null, expiration2, filter2, "id2", null, null,
        ENABLED, null);
    var iut = new BucketLifecycleConfiguration(Arrays.asList(rule1, rule2));
    serializeAndAssert(iut, testInfo);
  }

}
