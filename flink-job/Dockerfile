FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q DskipTests package

FROM flink:1.20.1-java17
COPY --from=build /src/target/flink-job-0.1.0.jar /opt/flink/usrlib/job.jar