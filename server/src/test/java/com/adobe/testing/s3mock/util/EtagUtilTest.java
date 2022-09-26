package com.adobe.testing.s3mock.util;

import static com.adobe.testing.s3mock.util.EtagUtil.normalizeEtag;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

class EtagUtilTest {

  @Test
  void testNormalize_null() {
    String etag = null;
    String normalizedEtag = normalizeEtag(etag);
    assertThat(normalizedEtag).isNull();
  }

  @Test
  void testNormalize_etagNoQuotes() {
    String etag = "some-etag";
    String normalizedEtag = normalizeEtag(etag);
    assertThat(normalizedEtag).isEqualTo("\"" + etag + "\"");
  }

  @Test
  void testNormalize_etagWithQuotes() {
    String etag = "\"some-etag\"";
    String normalizedEtag = normalizeEtag(etag);
    assertThat(normalizedEtag).isEqualTo(etag);
  }
}
