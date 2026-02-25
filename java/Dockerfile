FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /src
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre

WORKDIR /app
COPY --from=builder /src/target/ingestion-java-1.0.0.jar /app/ingest-benchmark.jar

ENTRYPOINT ["java", "-jar", "/app/ingest-benchmark.jar"]
