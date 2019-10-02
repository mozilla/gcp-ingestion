FROM maven:3-jdk-8 AS BASE
WORKDIR /app
COPY LICENSE /app/LICENSE
COPY pom.xml /app/pom.xml
COPY ingestion-core/pom.xml /app/ingestion-core/pom.xml
COPY ingestion-sink/pom.xml /app/ingestion-sink/pom.xml
COPY checkstyle /app/checkstyle
COPY ingestion-core/src /app/ingestion-core/src
COPY ingestion-sink/src /app/ingestion-sink/src

FROM base AS build
RUN mvn --activate-profiles ingestion-sink package

FROM base
COPY --from=build /app/ingestion-sink/target/*.jar /app/ingestion-sink/target/
CMD exec java -jar /app/ingestion-sink/target/*with-dependencies.jar
