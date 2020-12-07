ARG BASE_IMAGE_VERSION=20.04

FROM ubuntu:${BASE_IMAGE_VERSION} AS builder

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        openjdk-8-jdk-headless \
    && true

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

ADD . /code

RUN cd /code \
    && ./gradlew --no-daemon build \
    && true

FROM ubuntu:${BASE_IMAGE_VERSION}

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        openjdk-8-jre-headless \
        curl \
        net-tools \
        vim \
        less \
    && rm -rf /var/lib/apt/lists/* \
    && true

COPY --from=builder /code/build/libs/*.jar /app.jar
COPY --from=builder /code/src/main/resources/application.properties /application.properties

ENV LD_LIBRARY_PATH="/usr/local/lib"

WORKDIR /

ENTRYPOINT ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8090 -Xdebug", "-jar", "app.jar"]
