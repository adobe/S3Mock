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
package com.adobe.testing.s3mock.testcontainers

import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.nio.file.Path

/**
 * Testcontainer for S3Mock.
 */
class S3MockContainer(dockerImageName: DockerImageName) : GenericContainer<S3MockContainer>(dockerImageName) {
    /**
     * Create a S3MockContainer.
     *
     * @param tag in the format of "2.1.27"
     */
    constructor(tag: String) : this(DEFAULT_IMAGE_NAME.withTag(tag))

    /**
     * Create a S3MockContainer.
     *
     * @param dockerImageName in the format of [DockerImageName.parse] where the
     * parameter is the full image name like "adobe/s3mock:2.1.27"
     */
    init {
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME)
        addExposedPort(S3MOCK_DEFAULT_HTTP_PORT)
        addExposedPort(S3MOCK_DEFAULT_HTTPS_PORT)
        waitingFor(
            Wait.forHttp("/favicon.ico")
                .forPort(S3MOCK_DEFAULT_HTTP_PORT)
                .withMethod("GET")
                .forStatusCode(200)
        )
    }

    fun withRegion(region: String): S3MockContainer {
        this.addEnv(PROP_REGION, region)
        return self()
    }

    fun withRetainFilesOnExit(retainFilesOnExit: Boolean): S3MockContainer {
        this.addEnv(PROP_RETAIN_FILES_ON_EXIT, retainFilesOnExit.toString())
        return self()
    }

    fun withValidKmsKeys(kmsKeys: String): S3MockContainer {
        this.addEnv(PROP_VALID_KMS_KEYS, kmsKeys)
        return self()
    }

    fun withInitialBuckets(initialBuckets: String): S3MockContainer {
        this.addEnv(PROP_INITIAL_BUCKETS, initialBuckets)
        return self()
    }

    /**
     * Mount a volume from the host system for the S3Mock to use as the "root".
     * Docker must be able to read / write into this directory (!)
     *
     * @param root absolute path in host system
     */
    fun withVolumeAsRoot(root: String): S3MockContainer {
        this.withFileSystemBind(root, "/s3mockroot", BindMode.READ_WRITE)
        this.addEnv(PROP_ROOT_DIRECTORY, "/s3mockroot")
        return self()
    }

    /**
     * Mount a volume from the host system for the S3Mock to use as the "root".
     * Docker must be able to read / write into this directory (!)
     *
     * @param root absolute path in host system
     */
    fun withVolumeAsRoot(root: Path): S3MockContainer {
        return this.withVolumeAsRoot(root.toString())
    }

    val httpEndpoint: String
        get() = "http://$host:$httpServerPort"

    val httpsEndpoint: String
        get() = "https://$host:$httpsServerPort"

    val httpServerPort: Int
        get() = getMappedPort(S3MOCK_DEFAULT_HTTP_PORT)

    val httpsServerPort: Int
        get() = getMappedPort(S3MOCK_DEFAULT_HTTPS_PORT)

    companion object {
        const val IMAGE_NAME: String = "adobe/s3mock"
        private const val S3MOCK_DEFAULT_HTTP_PORT = 9090
        private const val S3MOCK_DEFAULT_HTTPS_PORT = 9191
        private val DEFAULT_IMAGE_NAME: DockerImageName = DockerImageName.parse(IMAGE_NAME)
        private const val PROP_INITIAL_BUCKETS = "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS"
        private const val PROP_ROOT_DIRECTORY = "COM_ADOBE_TESTING_S3MOCK_STORE_ROOT"
        private const val PROP_VALID_KMS_KEYS = "COM_ADOBE_TESTING_S3MOCK_STORE_VALID_KMS_KEYS"
        private const val PROP_REGION = "COM_ADOBE_TESTING_S3MOCK_STORE_REGION"
        private const val PROP_RETAIN_FILES_ON_EXIT = "COM_ADOBE_TESTING_S3MOCK_STORE_RETAIN_FILES_ON_EXIT"
    }
}
