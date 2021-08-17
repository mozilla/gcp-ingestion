FROM maven:3-jdk-11 AS base
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
# Enable cloud profiler by adding something like this to JAVA_OPTS:
#   -agentpath:/opt/cloud-profiler/profiler_java_agent.so=-cprof_service=bq-sink-loader,-cprof_service_version=20200504,-logtostderr,-minloglevel=1
# as described in the agent docs for java:
# https://cloud.google.com/profiler/docs/profiling-java#agent_configuration
# https://cloud.google.com/profiler/docs/profiling-java#agent_logging
RUN mkdir -p /opt/cloud-profiler && \
  wget -q -O- https://storage.googleapis.com/cloud-profiler/java/latest/profiler_java_agent.tar.gz \
  | tar xzv -C /opt/cloud-profiler
CMD exec java $JAVA_OPTS -jar /app/ingestion-sink/target/*.jar
