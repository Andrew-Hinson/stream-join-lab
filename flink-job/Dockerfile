FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -DskipTests package

FROM flink:1.20.1-java17
COPY --from=build /src/target/flink-job-0.1.0.jar /opt/flink/usrlib/job.jar
RUN cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.20.1.jar /opt/flink/lib/