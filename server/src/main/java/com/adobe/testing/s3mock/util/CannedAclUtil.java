/*
 *  Copyright 2017-2024 Adobe.
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

package com.adobe.testing.s3mock.util;

import static com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER;
import static com.adobe.testing.s3mock.dto.Owner.DEFAULT_OWNER_BUCKET;

import com.adobe.testing.s3mock.dto.AccessControlPolicy;
import com.adobe.testing.s3mock.dto.Grant;
import com.adobe.testing.s3mock.dto.Grantee;
import java.util.List;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * Utility class with helper methods to get canned ACLs.
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html">API Reference</a>
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl">API Reference</a>
 */
public class CannedAclUtil {

  private CannedAclUtil() {
    // private constructor for utility classes
  }

  public static AccessControlPolicy policyForCannedAcl(ObjectCannedACL cannedAcl) {
    return switch (cannedAcl) {
      case PRIVATE -> privateAcl();
      case PUBLIC_READ -> publicReadAcl();
      case PUBLIC_READ_WRITE -> publicReadWriteAcl();
      case AWS_EXEC_READ -> awsExecReadAcl();
      case AUTHENTICATED_READ -> authenticatedReadAcl();
      case BUCKET_OWNER_READ -> bucketOwnerReadAcl();
      case BUCKET_OWNER_FULL_CONTROL -> bucketOwnerFulleControlAcl();
      case UNKNOWN_TO_SDK_VERSION -> null;
    };
  }

  private static AccessControlPolicy bucketOwnerFulleControlAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            ),
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER_BUCKET.getId(),
                    DEFAULT_OWNER_BUCKET.getDisplayName(),
                    null, null),
                Grant.Permission.READ
            )
        )
    );
  }

  private static AccessControlPolicy bucketOwnerReadAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            ),
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER_BUCKET.getId(),
                    DEFAULT_OWNER_BUCKET.getDisplayName(),
                    null, null),
                Grant.Permission.READ
            )
        )
    );
  }

  private static AccessControlPolicy authenticatedReadAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            ),
            new Grant(
                new Grantee.Group(null, null,
                    null, Grantee.Group.AUTHENTICATED_USERS_URI),
                Grant.Permission.READ
            )
        )
    );
  }

  /**
   * The documentation says that EC2 gets READ access. Not sure what to configure for that.
   */
  private static AccessControlPolicy awsExecReadAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            )
        )
    );
  }

  private static AccessControlPolicy publicReadWriteAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            ),
            new Grant(
                new Grantee.Group(null, null,
                    null, Grantee.Group.ALL_USERS_URI),
                Grant.Permission.READ
            ),
            new Grant(
                new Grantee.Group(null, null,
                    null, Grantee.Group.ALL_USERS_URI),
                Grant.Permission.WRITE
            )
        )
    );
  }

  private static AccessControlPolicy publicReadAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            ),
            new Grant(
                new Grantee.Group(null, null,
                    null, Grantee.Group.ALL_USERS_URI),
                Grant.Permission.READ
            )
        )
    );
  }

  private static AccessControlPolicy privateAcl() {
    return new AccessControlPolicy(
        DEFAULT_OWNER,
        List.of(
            new Grant(
                new Grantee.CanonicalUser(DEFAULT_OWNER.getId(),
                    DEFAULT_OWNER.getDisplayName(),
                    null, null),
                Grant.Permission.FULL_CONTROL
            )
        )
    );
  }
}
