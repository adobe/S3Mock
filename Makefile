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

.DEFAULT_GOAL := help

# ─── Help ────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show available commands
	@echo "Usage: make <target>"
	@echo ""
	@awk 'BEGIN {FS=":.*## "}; /^[a-zA-Z0-9_-]+:.*## / { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─── Build ────────────────────────────────────────────────────────────────────

.PHONY: build
build: verify ## Full build — default target (alias: verify)

.PHONY: verify
verify: ## clean + verify (compile, test, lint, Docker)
	./mvnw -B -V -Dstyle.color=always clean verify

.PHONY: install
install: ## clean + install artifacts to local Maven repository
	./mvnw -B -V -Dstyle.color=always clean install

.PHONY: skip-docker
skip-docker: ## Full build with Docker skipped
	./mvnw -B -V -Dstyle.color=always clean install -DskipDocker

# ─── Code quality ─────────────────────────────────────────────────────────────

.PHONY: format
format: ## Auto-format Kotlin with ktlint (alias: fmt)
	./mvnw -B -V -Dstyle.color=always ktlint:format

.PHONY: fmt
fmt: format ## Auto-format Kotlin with ktlint (alias: format)

.PHONY: lint
lint: ## Check style without auto-fixing (ktlint + Checkstyle)
	./mvnw -B -V -Dstyle.color=always ktlint:check
	./mvnw -B -V -Dstyle.color=always checkstyle:check

.PHONY: typecheck
typecheck: ## Compile all modules without running tests
	./mvnw -B -V -Dstyle.color=always test-compile

.PHONY: check
check: lint typecheck test ## lint + typecheck + test

# ─── Testing ──────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Unit tests only (server/ module)
	./mvnw -B -V -Dstyle.color=always test -pl server

.PHONY: test-class
test-class: ## Run one test class: make test-class CLASS=BucketServiceTest
	./mvnw -B -V -Dstyle.color=always test -pl server -Dtest=$(CLASS)

.PHONY: integration-tests
integration-tests: ## Integration tests against a live Docker container
	./mvnw -B -V -Dstyle.color=always verify -pl integration-tests

.PHONY: integration-test-class
integration-test-class: ## Run one integration test: make integration-test-class CLASS=BucketIT
	./mvnw -B -V -Dstyle.color=always verify -pl integration-tests -Dit.test=$(CLASS)

# ─── Development ──────────────────────────────────────────────────────────────

.PHONY: run
run: ## Run S3Mock from source via Spring Boot
	./mvnw spring-boot:run -pl server

.PHONY: sort
sort: ## Sort POM files with sortpom
	./mvnw -B -V -Dstyle.color=always com.github.ekryd.sortpom:sortpom-maven-plugin:sort

.PHONY: release
release: ## Prepare and perform a Maven release (CI use)
	./mvnw -B -V release:prepare release:perform
