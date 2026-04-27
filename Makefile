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

# Agents: run only make targets listed here. No direct shell commands.

.PHONY: build verify install skip-docker format fmt lint typecheck check integration-tests run test test-class sort help
.DEFAULT_GOAL := build

build: verify

verify:
	./mvnw -B -V -Dstyle.color=always clean verify

install:
	./mvnw -B -V -Dstyle.color=always clean install

skip-docker:
	./mvnw -B -V -Dstyle.color=always clean install -DskipDocker

# Auto-format Kotlin source with ktlint. Run before committing.
format:
	./mvnw -B -V -Dstyle.color=always ktlint:format

# Alias for format.
fmt: format

# Check style without auto-fixing: ktlint (Kotlin) then Checkstyle (Java/XML).
lint:
	./mvnw -B -V -Dstyle.color=always ktlint:check
	./mvnw -B -V -Dstyle.color=always checkstyle:check

# Compile all modules (main + test sources) without running tests.
# Kotlin/Java compilation is the type check — no separate type checker exists.
typecheck:
	./mvnw -B -V -Dstyle.color=always test-compile

# Unit tests only (server/ module). For integration tests use: make integration-tests
test:
	./mvnw -B -V -Dstyle.color=always test -pl server

# Run a single test class: make test-class CLASS=BucketServiceTest
test-class:
	./mvnw -B -V -Dstyle.color=always test -pl server -Dtest=$(CLASS)

integration-tests:
	./mvnw -B -V -Dstyle.color=always verify -pl integration-tests

# Master validation target: lint + typecheck + unit tests.
check: lint typecheck test

run:
	./mvnw spring-boot:run -pl server

sort:
	./mvnw -B -V -Dstyle.color=always com.github.ekryd.sortpom:sortpom-maven-plugin:sort

help:
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build"
	@echo "  build              Full build — default target (alias: verify)"
	@echo "  verify             clean + verify (compile, test, lint, Docker)"
	@echo "  install            clean + install artifacts to local Maven repository"
	@echo "  skip-docker        Full build with Docker skipped"
	@echo ""
	@echo "Code quality"
	@echo "  lint               Check style without auto-fixing (ktlint + Checkstyle)"
	@echo "  fmt                Auto-format Kotlin with ktlint (alias: format)"
	@echo "  format             Auto-format Kotlin with ktlint (alias: fmt)"
	@echo "  typecheck          Compile all modules without running tests"
	@echo "  check              lint + typecheck + test"
	@echo ""
	@echo "Testing"
	@echo "  test               Unit tests only (server/ module)"
	@echo "  test-class         Run one test class: make test-class CLASS=BucketServiceTest"
	@echo "  integration-tests  Integration tests against a live Docker container"
	@echo ""
	@echo "Development"
	@echo "  run                Run S3Mock from source via Spring Boot"
	@echo "  sort               Sort POM files with sortpom"
	@echo "  help               Show this message"
	@echo ""
