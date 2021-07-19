/*
 *  Copyright 2017-2021 Adobe.
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.adobe.testing.s3mock.dto.ObjectRef;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Verifies parsing behaviour from {@link ObjectRef}.
 */
public class ObjectRefTest {

  private static final String BUCKET = UUID.randomUUID().toString();
  private static final String KEY = UUID.randomUUID().toString();
  private static final String VALID_COPY_SOURCE = BUCKET + DELIMITER + KEY;

  @Test
  public void fromPrefixedCopySourceString() {
    final ObjectRef objectRef = new ObjectRef(DELIMITER + VALID_COPY_SOURCE);

    assertThat(objectRef.getBucket()).isEqualTo(BUCKET);
    assertThat(objectRef.getKey()).isEqualTo(KEY);
  }

  @Test
  public void fromCopySourceString() {
    final ObjectRef objectRef = new ObjectRef(VALID_COPY_SOURCE);

    assertThat(objectRef.getBucket()).isEqualTo(BUCKET);
    assertThat(objectRef.getKey()).isEqualTo(KEY);
  }

  @Test
  public void invalidCopySource() {
    Throwable thrown = catchThrowable(() -> new ObjectRef(UUID.randomUUID().toString()));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nullCopySource() {
    Throwable thrown = catchThrowable(() -> new ObjectRef(null));
    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }
}
