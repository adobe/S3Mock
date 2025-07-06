.PHONY: build verify install
.DEFAULT_GOAL := build

build: verify

verify:
	./mvnw -B -V -Dstyle.color=always clean verify

install:
	./mvnw -B -V -Dstyle.color=always clean install
