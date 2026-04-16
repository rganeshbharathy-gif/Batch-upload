# ─── Build stage ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /workspace

COPY pom.xml .
COPY src ./src

# Install Maven, pre-fetch deps for layer caching, then build the fat JAR
RUN apt-get update && apt-get install -y --no-install-recommends maven && \
    rm -rf /var/lib/apt/lists/* && \
    mvn -q dependency:go-offline && \
    mvn -q package -DskipTests

# ─── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app

RUN groupadd -r batchgroup && useradd -r -g batchgroup batchuser

COPY --from=builder /workspace/target/batch-upload-*.jar app.jar

RUN mkdir -p /data && chown batchuser:batchgroup /data

USER batchuser

EXPOSE 8080

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+UseG1GC", \
  "-XX:MaxGCPauseMillis=200", \
  "-jar", "app.jar"]
