/*
 *  Copyright 2017 Adobe.
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

package com.adobe.testing.s3mock.domain;

import static com.adobe.testing.s3mock.dto.ObjectRef.DELIMITER;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.adobe.testing.s3mock.dto.ObjectRef;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Verifies parsing behaviour from {@link ObjectRef}.
 */
public class ObjectRefTest {
  private static final String BUCKET = UUID.randomUUID().toString();
  private static final String KEY = UUID.randomUUID().toString();
  private static final String VALID_COPY_SOURCE = BUCKET + DELIMITER + KEY;

  @Rule
  public ExpectedException expectedExceptions = ExpectedException.none();

  @Test
  public void fromPrefixedCopySourceString() {
    final ObjectRef objectRef = ObjectRef.from(DELIMITER + VALID_COPY_SOURCE);

    assertThat(objectRef.getBucket(), is(BUCKET));
    assertThat(objectRef.getKey(), is(KEY));
  }


  @Test
  public void fromCopySourceString() {
    final ObjectRef objectRef = ObjectRef.from(VALID_COPY_SOURCE);

    assertThat(objectRef.getBucket(), is(BUCKET));
    assertThat(objectRef.getKey(), is(KEY));
  }

  @Test
  public void invalidCopySource() {
    expectedExceptions.expect(IllegalArgumentException.class);
    ObjectRef.from(UUID.randomUUID().toString());
  }

  @Test
  public void nullCopySource() {
    expectedExceptions.expect(NullPointerException.class);
    ObjectRef.from(null);
  }
}
