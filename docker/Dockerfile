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

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS staging_area

RUN apk --no-cache add openjdk25-jdk openjdk25-jmods

ENV JAVA_MINIMAL="/opt/java-minimal"

# prepare dependencies for jdeps/jlink
COPY ./target/s3mock-exec.jar s3mock.jar
# Spring Boot uses fat JARs, jdeps can't read them. Need to unpack JAR before running jdeps
ENV TMP_DIR="/tmp/app-jar"
RUN mkdir -p ${TMP_DIR}
RUN unzip -q s3mock.jar -d "${TMP_DIR}"

# find JDK dependencies dynamically from unpacked jar
RUN jdeps \
# don't worry about missing modules
--ignore-missing-deps \
# suppress any warnings printed to console
-q \
# java release version targeting
--multi-release 25 \
# output the dependencies at end of run
--print-module-deps \
--recursive \
# specify the the dependencies for the jar
--class-path "${TMP_DIR}/BOOT-INF/lib/*:${TMP_DIR}/BOOT-INF/classes:${TMP_DIR}" \
# pipe the result of running jdeps on the app jar to file
${TMP_DIR}/org ${TMP_DIR}/BOOT-INF/classes ${TMP_DIR}/BOOT-INF/lib/*.jar > jre-deps.info

# build minimal JRE
RUN jlink \
    --verbose \
    --add-modules $(cat jre-deps.info),jdk.crypto.ec \
    --compress 2 \
    --no-header-files \
    --no-man-pages \
    --strip-java-debug-attributes \
    --output "$JAVA_MINIMAL"

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

ENV JAVA_HOME=/opt/java-minimal
ENV PATH="$PATH:$JAVA_HOME/bin"

COPY --from=staging_area "$JAVA_HOME" "$JAVA_HOME"
COPY ./target/s3mock-exec.jar s3mock.jar

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
ENV root=/s3mockroot

EXPOSE 9090 9191

# run the app on startup
ENTRYPOINT ["java", "--illegal-access=warn", "-Djava.security.egd=file:/dev/./urandom", "-XX:+UseZGC", "-XX:+ZGenerational", "-jar", "s3mock.jar" ]
