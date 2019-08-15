FROM maven:3-jdk-8 AS BASE
WORKDIR /app/ingestion-sink
COPY pom.xml /app/
COPY ingestion-sink/pom.xml ./
COPY checkstyle/ /app/checkstyle/
COPY ingestion-sink/src /app/ingestion-sink/src
RUN ln -s ../checkstyle

FROM base AS build
RUN mvn package

FROM base
COPY --from=build /app/ingestion-sink/target/*.jar target/
CMD exec java -jar target/*with-dependencies.jar
