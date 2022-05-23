#
#  Copyright 2017-2022 Adobe.
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

FROM alpine:3.16.0 as staging_area

RUN apk --no-cache add openjdk17-jdk openjdk17-jmods

ENV JAVA_MINIMAL="/opt/java-minimal"

# prepare dependencies for jdeps/jlink
COPY ./target/s3mock-exec.jar s3mock.jar
# Spring Boot uses fat JARs, jdeps can't read them. Need to unpack JAR before running jdeps
ENV TMP_DIR="/tmp/app-jar"
RUN mkdir -p ${TMP_DIR}
RUN unzip -q s3mock.jar -d "${TMP_DIR}"

#TODO: running jdeps does not work with spring-boot 2.5 or later.
## find JDK dependencies dynamically from unpacked jar
#RUN jdeps \
## dont worry about missing modules
#--ignore-missing-deps \
## suppress any warnings printed to console
#-q \
## java release version targeting
#--multi-release 17 \
## output the dependencies at end of run
#--print-module-deps \
#--recursive \
## specify the the dependencies for the jar
#--class-path ${TMP_DIR}/BOOT-INF/lib/* ${TMP_DIR}/BOOT-INF/classes ${TMP_DIR} \
## pipe the result of running jdeps on the app jar to file
#${TMP_DIR}/org ${TMP_DIR}/BOOT-INF/classes ${TMP_DIR}/BOOT-INF/lib/*.jar > jre-deps.info \
#
## build minimal JRE
#RUN jlink \
#    --verbose \
#    --add-modules $(cat jre-deps.info) \
#    --compress 2 \
#    --no-header-files \
#    --no-man-pages \
#    --strip-java-debug-attributes \
#    --output "$JAVA_MINIMAL"

#TODO: creating "static" minimal JRE here. Activate jdeps again later
# create a minimal jdk assembly with those modules that we need
RUN jlink \
    --module-path $JAVA_HOME/jmods \
    --verbose \
    --add-modules java.base,java.logging,java.xml,jdk.unsupported,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument \
    --compress 2 \
    --no-header-files \
    --no-man-pages \
    --strip-java-debug-attributes \
    --output "$JAVA_MINIMAL"

FROM alpine:3.16.0

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
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-Xmx128m", "--illegal-access=warn", "-Djava.security.egd=file:/dev/./urandom", "-jar", "s3mock.jar"]
