FROM maven:3-jdk-8 AS base
WORKDIR /app/ingestion-sink
COPY LICENSE /app/LICENSE
COPY pom.xml /app/pom.xml
COPY ingestion-sink/pom.xml /app/ingestion-sink/pom.xml
COPY checkstyle /app/checkstyle
COPY ingestion-sink/checkstyle /app/ingestion-sink/checkstyle
COPY ingestion-core/src /app/ingestion-core/src
COPY ingestion-sink/src /app/ingestion-sink/src
COPY ingestion-sink/version.json /app/version.json

FROM base AS build
RUN mvn package

FROM base
COPY --from=build /app/ingestion-sink/target/*.jar /app/ingestion-sink/target/
CMD exec java $JAVA_OPTS -jar /app/ingestion-sink/target/*with-dependencies.jar
