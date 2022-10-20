# Use maven image to build the jars that will be included via COPY later.
FROM maven:3-jdk-11 AS build
WORKDIR /app/ingestion-beam
COPY pom.xml /app/pom.xml
COPY ingestion-beam/pom.xml /app/ingestion-beam/pom.xml
COPY checkstyle /app/checkstyle
COPY ingestion-beam/checkstyle /app/ingestion-sink/checkstyle
RUN mvn prepare-package
COPY ingestion-core/src /app/ingestion-core/src
COPY ingestion-beam/src /app/ingestion-beam/src
RUN mvn package -Dmaven.test.skip=true -Dmaven.compiler.release=11

# This is the base image for the final image we're building.
# The base image documentation is here:
# https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#changing_the_base_image
# and the versions available are defined here:
# https://cloud.google.com/dataflow/docs/reference/flex-templates-base-images
FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:20220418_RC00
ARG PACKAGE_VERSION=0.1-SNAPSHOT
COPY --from=build /app/ingestion-beam/target/ingestion-beam-$PACKAGE_VERSION.lib/ /template/ingestion-beam-$PACKAGE_VERSION.lib/
# copy jar after dependencies to maximize docker caching and reduce upload size
COPY --from=build /app/ingestion-beam/target/ingestion-beam-$PACKAGE_VERSION.jar /template/
ENV FLEX_TEMPLATE_JAVA_CLASSPATH=/template/ingestion-beam-$PACKAGE_VERSION.jar
ARG FLEX_TEMPLATE_JAVA_MAIN_CLASS
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS=${FLEX_TEMPLATE_JAVA_MAIN_CLASS}
