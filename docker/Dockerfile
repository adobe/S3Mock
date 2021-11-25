#
#  Copyright 2017-2021 Adobe.
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

FROM alpine:3.15.0 as staging_area

RUN apk --no-cache add openjdk17-jdk openjdk17-jmods

ENV JAVA_MINIMAL="/opt/java-minimal"

# build minimal JRE
RUN /usr/lib/jvm/java-17-openjdk/bin/jlink \
    --verbose \
    --add-modules java.base,java.logging,java.xml,jdk.unsupported,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument \
    --compress 2 \
    --no-header-files \
    --no-man-pages \
    --strip-java-debug-attributes \
    --output "$JAVA_MINIMAL"

FROM alpine:3.15.0

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
ENTRYPOINT java -XX:+UseContainerSupport -Xmx128m --illegal-access=warn -Djava.security.egd=file:/dev/./urandom -jar s3mock.jar
