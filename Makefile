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

.PHONY: build verify install skip-docker format integration-tests run test sort
.DEFAULT_GOAL := build

build: verify

verify:
	./mvnw -B -V -Dstyle.color=always clean verify

install:
	./mvnw -B -V -Dstyle.color=always clean install

skip-docker:
	./mvnw -B -V -Dstyle.color=always clean install -DskipDocker

format:
	./mvnw -B -V -Dstyle.color=always ktlint:format

integration-tests:
	./mvnw -B -V -Dstyle.color=always verify -pl integration-tests

run:
	./mvnw spring-boot:run -pl server

test:
	./mvnw -B -V -Dstyle.color=always test -pl server

sort:
	./mvnw -B -V -Dstyle.color=always com.github.ekryd.sortpom:sortpom-maven-plugin:sort
