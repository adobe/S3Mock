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

import static com.adobe.testing.s3mock.dto.CopySource.DELIMITER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Verifies parsing behaviour from {@link CopySource}.
 */
class CopySourceTest {

  private static final String BUCKET = UUID.randomUUID().toString();
  private static final String KEY = UUID.randomUUID().toString();
  private static final String VALID_COPY_SOURCE = BUCKET + DELIMITER + KEY;

  @Test
  void fromPrefixedCopySourceString() {
    var copySource = new CopySource(DELIMITER + VALID_COPY_SOURCE);

    assertThat(copySource.bucket()).isEqualTo(BUCKET);
    assertThat(copySource.key()).isEqualTo(KEY);
  }

  @Test
  void fromCopySourceString() {
    var copySource = new CopySource(VALID_COPY_SOURCE);

    assertThat(copySource.bucket()).isEqualTo(BUCKET);
    assertThat(copySource.key()).isEqualTo(KEY);
  }

  @Test
  void invalidCopySource() {
    var thrown = catchThrowable(() -> new CopySource(UUID.randomUUID().toString()));
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nullCopySource() {
    var thrown = catchThrowable(() -> new CopySource(null));
    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }
}
