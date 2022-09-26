package com.adobe.testing.s3mock.util;

public class EtagUtil {

  /**
   * Returns etag in normalized form with surrounding quotes.
   * This normalized form is persisted so that S3Mock can conform to RFC2616 / RFC7232.
   * <a href="https://www.rfc-editor.org/rfc/rfc2616#section-14.19">RFC2616</a>
   * <a href="https://www.rfc-editor.org/rfc/rfc7232">RFC7232</a>
   */
  public static String normalizeEtag(String etag) {
    if (etag == null) {
      return null;
    } else if (etag.startsWith("\"") && etag.endsWith("\"")) {
      return etag;
    } else {
      return String.format("\"%s\"", etag);
    }
  }
}
