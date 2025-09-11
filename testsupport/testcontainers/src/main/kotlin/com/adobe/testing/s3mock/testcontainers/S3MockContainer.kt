/*
 *  Copyright 2017-2025 Adobe.
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

package com.adobe.testing.s3mock.testcontainers;

import java.nio.file.Path;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainer for S3Mock.
 */
public class S3MockContainer extends GenericContainer<S3MockContainer> {
  public static final String IMAGE_NAME = "adobe/s3mock";
  private static final int S3MOCK_DEFAULT_HTTP_PORT = 9090;
  private static final int S3MOCK_DEFAULT_HTTPS_PORT = 9191;
  private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(IMAGE_NAME);
  private static final String PROP_INITIAL_BUCKETS = "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS";
  private static final String PROP_ROOT_DIRECTORY = "COM_ADOBE_TESTING_S3MOCK_STORE_ROOT";
  private static final String PROP_VALID_KMS_KEYS = "COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS";
  private static final String PROP_REGION = "COM_ADOBE_TESTING_S3MOCK_STORE_REGION";
  private static final String PROP_RETAIN_FILES_ON_EXIT = "COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT";

  /**
   * Create a S3MockContainer.
   *
   * @param tag in the format of "2.1.27"
   */
  public S3MockContainer(String tag) {
    this(DEFAULT_IMAGE_NAME.withTag(tag));
  }

  /**
   * Create a S3MockContainer.
   *
   * @param dockerImageName in the format of {@link DockerImageName#parse(String)} where the
   *                        parameter is the full image name like "adobe/s3mock:2.1.27"
   */
  public S3MockContainer(DockerImageName dockerImageName) {
    super(dockerImageName);

    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
    addExposedPort(S3MOCK_DEFAULT_HTTP_PORT);
    addExposedPort(S3MOCK_DEFAULT_HTTPS_PORT);
    waitingFor(Wait.forHttp("/favicon.ico")
        .forPort(S3MOCK_DEFAULT_HTTP_PORT)
        .withMethod("GET")
        .forStatusCode(200));
  }

  public S3MockContainer withRegion(String region) {
    this.addEnv(PROP_REGION, region);
    return self();
  }

  public S3MockContainer withRetainFilesOnExit(boolean retainFilesOnExit) {
    this.addEnv(PROP_RETAIN_FILES_ON_EXIT, String.valueOf(retainFilesOnExit));
    return self();
  }

  public S3MockContainer withValidKmsKeys(String kmsKeys) {
    this.addEnv(PROP_VALID_KMS_KEYS, kmsKeys);
    return self();
  }

  public S3MockContainer withInitialBuckets(String initialBuckets) {
    this.addEnv(PROP_INITIAL_BUCKETS, initialBuckets);
    return self();
  }

  /**
   * Mount a volume from the host system for the S3Mock to use as the "root".
   * Docker must be able to read / write into this directory (!)
   *
   * @param root absolute path in host system
   */
  public S3MockContainer withVolumeAsRoot(String root) {
    this.withFileSystemBind(root, "/s3mockroot", BindMode.READ_WRITE);
    this.addEnv(PROP_ROOT_DIRECTORY, "/s3mockroot");
    return self();
  }

  /**
   * Mount a volume from the host system for the S3Mock to use as the "root".
   * Docker must be able to read / write into this directory (!)
   *
   * @param root absolute path in host system
   */
  public S3MockContainer withVolumeAsRoot(Path root) {
    return this.withVolumeAsRoot(root.toString());
  }

  public String getHttpEndpoint() {
    return String.format("http://%s:%d", getHost(), getHttpServerPort());
  }

  public String getHttpsEndpoint() {
    return String.format("https://%s:%d", getHost(), getHttpsServerPort());
  }

  public Integer getHttpServerPort() {
    return getMappedPort(S3MOCK_DEFAULT_HTTP_PORT);
  }

  public Integer getHttpsServerPort() {
    return getMappedPort(S3MOCK_DEFAULT_HTTPS_PORT);
  }
}
