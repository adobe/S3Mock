#!/bin/sh
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

BUILDER_NAME=$1
MAJOR_VERSION_TAG_NAME=$2
MINOR_VERSION_TAG_NAME=$3
PATCH_VERSION_TAG_NAME=$4
LATEST_TAG_NAME=$5


# build --load to make the Docker container available in the local architecture for local
# integration tests.
docker buildx build --load --tag "${PATCH_VERSION_TAG_NAME}" --tag "${LATEST_TAG_NAME}" --tag "${MAJOR_VERSION_TAG_NAME}" --tag "${MINOR_VERSION_TAG_NAME}" --builder "${BUILDER_NAME}" .
