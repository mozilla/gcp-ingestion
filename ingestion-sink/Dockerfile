FROM maven:3-jdk-8 AS base
WORKDIR /app/ingestion-sink

FROM base AS build
COPY LICENSE /app/LICENSE
COPY pom.xml /app/pom.xml
COPY ingestion-sink/pom.xml /app/ingestion-sink/pom.xml
COPY checkstyle /app/checkstyle
COPY ingestion-sink/checkstyle /app/ingestion-sink/checkstyle
RUN mvn prepare-package
COPY ingestion-core/src /app/ingestion-core/src
COPY ingestion-sink/src /app/ingestion-sink/src
COPY ingestion-sink/version.json /app/version.json
RUN mvn package
# delete everything from target that isn't the package
RUN find target -mindepth 1 -not -name '*'.jar -not -name '*.lib' -delete

FROM base
COPY --from=build /app/ /app/
CMD exec java $JAVA_OPTS -jar /app/ingestion-sink/target/*.jar
