FROM maven:3-jdk-8 AS base
WORKDIR /app/ingestion-beam

COPY LICENSE /app/LICENSE
COPY pom.xml /app/pom.xml
COPY ingestion-beam/pom.xml /app/ingestion-beam/pom.xml
COPY checkstyle /app/checkstyle
COPY ingestion-beam/checkstyle /app/ingestion-beam/checkstyle
COPY ingestion-core/src /app/ingestion-core/src
COPY ingestion-beam/src /app/ingestion-beam/src
COPY ingestion-beam/bin /app/ingestion-beam/bin

FROM base AS build
RUN /app/ingestion-beam/bin/download-cities15000
RUN /app/ingestion-beam/bin/download-geolite2
RUN /app/ingestion-beam/bin/download-schemas
RUN mvn package

FROM base
COPY --from=build /app/ingestion-beam/target/*.jar /app/ingestion-beam/target/
RUN apt-get update && apt-get install -y python3 python3-pip jq
RUN pip3 install google-cloud-bigquery