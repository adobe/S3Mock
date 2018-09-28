#
#  Copyright 2017-2018 Adobe.
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

FROM adoptopenjdk/openjdk11:${adoptopenjdk11.image.version} as staging_area

# create a minimal jdk assembly with those modules that we need
RUN jlink \
    --module-path $JAVA_HOME/jmods \
    --verbose \
    --add-modules java.base,java.logging,java.xml,jdk.unsupported,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument \
    --compress 2 \
    --no-header-files \
    --no-man-pages \
    --strip-debug \
    --output /target/opt/jdk-minimal

# add libz* files, so that we don't have to re-install that (not included in alpine-glibc)
RUN mkdir -p /target/usr/glibc-compat/lib; \
    cp /usr/glibc-compat/lib/libz* /target/usr/glibc-compat/lib

#
# build the actual image
#
FROM frolvlad/alpine-glibc:${alpine-glibc.image.version}

# copy files prepared in the other container
COPY --from=staging_area /target /

# rebuild ld.so.cache to add libz
RUN /usr/glibc-compat/sbin/ldconfig

ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8 \
    JAVA_HOME=/opt/jdk-minimal \
    PATH="$PATH:/opt/jdk-minimal/bin"

EXPOSE 9090 9191

COPY maven /opt/

ENTRYPOINT java -XX:+UseContainerSupport -Xmx128m --illegal-access=warn -Djava.security.egd=file:/dev/./urandom -jar /opt/service/*.jar
