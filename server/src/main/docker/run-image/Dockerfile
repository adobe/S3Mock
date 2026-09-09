#
#  Copyright 2017-2026 Adobe.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Custom Cloud Native Buildpacks run image for the S3Mock OCI image.
#
# It extends the same BellSoft Alpaquita Linux (musl) hardened run image that the
# `bellsoft/buildpacks.builder:musl` builder uses by default, and only adds an empty
# `/s3mockroot` directory owned by the non-root `cnb` user that the image runs as.
#
# Why this exists (see https://github.com/adobe/S3Mock/issues/3139):
# Buildpacks-built images run as the non-root `cnb` user. When a Docker *named volume* is
# mounted onto a directory that already exists in the image, Docker initialises the fresh
# volume with that directory's ownership. By pre-creating `/s3mockroot` owned by `cnb`, a named
# volume mounted at `/s3mockroot` is writable by S3Mock without having to run the container as
# root — so `COM_ADOBE_TESTING_S3MOCK_STORE_ROOT=/s3mockroot` persists data across restarts.
#
# The FROM tag is kept in sync with the builder's run image by Dependabot.
FROM bellsoft/buildpacks.hardened-run:musl

# CNB_USER_ID / CNB_GROUP_ID are provided by the run image (both 1000, the `cnb` user).
USER root
RUN mkdir -p /s3mockroot && chown "${CNB_USER_ID}:${CNB_GROUP_ID}" /s3mockroot

# Restore the non-root user the buildpack lifecycle and the launched app run as.
USER ${CNB_USER_ID}:${CNB_GROUP_ID}

# The default Paketo/Buildpacks launch working directory is /workspace, whereas the pre-5.2
# root-based image had no explicit WORKDIR (defaulting to /). A *relative*
# COM_ADOBE_TESTING_S3MOCK_STORE_ROOT therefore silently resolved to a different absolute path
# after the 5.2 migration (e.g. "s3root" -> /workspace/s3root instead of /s3root): the app still
# started and served requests successfully, writing into a directory a mounted volume never
# touched, so persisted data was silently lost on container recreation. Restoring "/" as the
# working directory keeps relative store roots resolving exactly as they did before 5.2.
WORKDIR /
