/*
 *  Copyright 2017-2026 Adobe.
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
package com.adobe.testing.s3mock.testcontainers

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.Volume
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.nio.file.Path

/**
 * Testcontainer for S3Mock.
 */
class S3MockContainer(
  dockerImageName: DockerImageName,
) : GenericContainer<S3MockContainer>(dockerImageName) {
  /** Accumulates Spring profiles so [withSpringProfiles]/[withVectors]/[withDebug] compose instead of overwriting. */
  private val activeProfiles = linkedSetOf<String>()

  /**
   * Create a S3MockContainer.
   *
   * @param tag in the format of "2.1.27"
   */
  constructor(tag: String) : this(DEFAULT_IMAGE_NAME.withTag(tag))

  init {
    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME)
    addExposedPort(S3MOCK_DEFAULT_HTTP_PORT)
    addExposedPort(S3MOCK_DEFAULT_HTTPS_PORT)
    addExposedPort(S3MOCK_DEFAULT_VECTORS_HTTP_PORT)
    addExposedPort(S3MOCK_DEFAULT_VECTORS_HTTPS_PORT)
    waitingFor(
      Wait
        .forHttp("/favicon.ico")
        .forPort(S3MOCK_DEFAULT_HTTP_PORT)
        .withMethod("GET")
        .forStatusCode(200),
    )
  }

  fun withRegion(region: String): S3MockContainer = withEnv(PROP_REGION, region)

  /**
   * Add one or more Spring profiles to the container's `SPRING_PROFILES_ACTIVE` env var.
   *
   * Profiles compose: repeated calls (and helpers such as [withVectors]) accumulate into a single
   * comma-separated value rather than overwriting each other, and duplicates are ignored.
   */
  fun withSpringProfiles(vararg profiles: String): S3MockContainer =
    apply {
      activeProfiles.addAll(profiles.map { it.trim() }.filter { it.isNotEmpty() })
      withEnv(PROP_SPRING_PROFILES_ACTIVE, activeProfiles.joinToString(","))
    }

  /**
   * Activate the `vectors` Spring profile so the container serves the S3 Vectors API on
   * [vectorsHttpEndpoint] / [vectorsHttpsEndpoint]. Without this, the vectors ports are exposed
   * but nothing listens on them.
   *
   * Composes with other profiles — see [withSpringProfiles].
   */
  fun withVectors(): S3MockContainer = withSpringProfiles(PROFILE_VECTORS)

  /**
   * Activate the server's `debug` Spring profile, which raises logging to debug level and (via the
   * server's `spring.profiles.group.debug=actuator` grouping) also activates the `actuator` profile.
   *
   * Composes with other profiles — see [withSpringProfiles].
   */
  fun withDebug(): S3MockContainer = withSpringProfiles(PROFILE_DEBUG)

  fun withRetainFilesOnExit(retainFilesOnExit: Boolean): S3MockContainer = withEnv(PROP_RETAIN_FILES_ON_EXIT, retainFilesOnExit.toString())

  fun withValidKmsKeys(kmsKeys: String): S3MockContainer = withEnv(PROP_VALID_KMS_KEYS, kmsKeys)

  fun withInitialBuckets(initialBuckets: String): S3MockContainer = withEnv(PROP_INITIAL_BUCKETS, initialBuckets)

  /**
   * Mount a Docker *named volume* as the S3Mock store root ([STORE_ROOT_MOUNT_PATH]) so data
   * persists across container restarts **without** running the container as root.
   *
   * The S3Mock OCI image pre-creates [STORE_ROOT_MOUNT_PATH] owned by the non-root `cnb` user it
   * runs as. Docker initialises a fresh named volume with that directory's ownership, so the
   * non-root process can write into the mounted volume — unlike a bind mount, which keeps its host
   * ownership and therefore requires [withVolumeAsRoot]. Combine with [withRetainFilesOnExit] to
   * keep the data after the container shuts down.
   *
   * Docker creates the named volume on demand if it does not exist yet. The caller owns its
   * lifecycle (e.g. removing it afterwards).
   *
   * @param volumeName name of the Docker named volume to mount
   */
  fun withNamedVolume(volumeName: String): S3MockContainer {
    withEnv(PROP_ROOT_DIRECTORY, STORE_ROOT_MOUNT_PATH)
    return withCreateContainerCmdModifier { cmd ->
      val hostConfig = cmd.hostConfig ?: HostConfig.newHostConfig()
      val existing = hostConfig.binds ?: emptyArray()
      hostConfig.withBinds(*existing, Bind(volumeName, Volume(STORE_ROOT_MOUNT_PATH)))
      cmd.withHostConfig(hostConfig)
    }
  }

  /**
   * Mount a volume from the host system for the S3Mock to use as the "root".
   * Docker must be able to read / write into this directory (!)
   *
   * The container is forced to run as `root` for this mount because the S3Mock OCI image is built
   * by Cloud Native Buildpacks and runs as the non-root `cnb` user by default. On Linux, a
   * bind-mounted host directory keeps its host ownership, so the non-root user cannot write into it
   * and every write fails with an HTTP 500. Running as `root` sidesteps the host/container UID
   * mismatch and restores the writable behaviour of the previous root-based image.
   *
   * For a Docker *named volume* prefer [withNamedVolume], which stays non-root.
   *
   * @param root absolute path in host system
   */
  fun withVolumeAsRoot(root: String): S3MockContainer {
    withEnv(PROP_ROOT_DIRECTORY, STORE_ROOT_MOUNT_PATH)
    withCreateContainerCmdModifier { it.withUser("0:0") }
    return withFileSystemBind(root, STORE_ROOT_MOUNT_PATH, BindMode.READ_WRITE)
  }

  /**
   * Mount a volume from the host system for the S3Mock to use as the "root".
   * Docker must be able to read / write into this directory (!)
   *
   * @param root absolute path in host system
   */
  fun withVolumeAsRoot(root: Path): S3MockContainer = withVolumeAsRoot(root.toString())

  val httpEndpoint: String
    get() = "http://$host:$httpServerPort"

  val httpsEndpoint: String
    get() = "https://$host:$httpsServerPort"

  val httpServerPort: Int
    get() = getMappedPort(S3MOCK_DEFAULT_HTTP_PORT)

  val httpsServerPort: Int
    get() = getMappedPort(S3MOCK_DEFAULT_HTTPS_PORT)

  val vectorsHttpEndpoint: String
    get() = "http://$host:$vectorsHttpServerPort"

  val vectorsHttpsEndpoint: String
    get() = "https://$host:$vectorsHttpsServerPort"

  val vectorsHttpServerPort: Int
    get() = getMappedPort(S3MOCK_DEFAULT_VECTORS_HTTP_PORT)

  val vectorsHttpsServerPort: Int
    get() = getMappedPort(S3MOCK_DEFAULT_VECTORS_HTTPS_PORT)

  companion object {
    const val IMAGE_NAME: String = "adobe/s3mock"
    private const val S3MOCK_DEFAULT_HTTP_PORT = 9090
    private const val S3MOCK_DEFAULT_HTTPS_PORT = 9191
    private const val S3MOCK_DEFAULT_VECTORS_HTTP_PORT = 9092
    private const val S3MOCK_DEFAULT_VECTORS_HTTPS_PORT = 9193
    private val DEFAULT_IMAGE_NAME: DockerImageName = DockerImageName.parse(IMAGE_NAME)

    /**
     * Store-root mount point pre-created (owned by the non-root `cnb` user) in the S3Mock OCI image.
     * Used by [withNamedVolume] and [withVolumeAsRoot] as the in-container store-root path.
     */
    private const val STORE_ROOT_MOUNT_PATH = "/s3mockroot"

    private const val PROP_SPRING_PROFILES_ACTIVE = "SPRING_PROFILES_ACTIVE"
    private const val PROFILE_VECTORS = "vectors"
    private const val PROFILE_DEBUG = "debug"

    /**
     * These properties are Spring Boot's standard env var relaxed binding of the same StoreProperties fields defined in the server module and must be kept in sync.
     */
    private const val PROP_INITIAL_BUCKETS = "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS"
    private const val PROP_ROOT_DIRECTORY = "COM_ADOBE_TESTING_S3MOCK_STORE_ROOT"
    private const val PROP_VALID_KMS_KEYS = "COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS"
    private const val PROP_REGION = "COM_ADOBE_TESTING_S3MOCK_STORE_REGION"
    private const val PROP_RETAIN_FILES_ON_EXIT = "COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT"
  }
}
