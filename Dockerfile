# ─── Build stage ──────────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jdk-alpine AS builder
WORKDIR /workspace

COPY pom.xml .
COPY src ./src

# Download dependencies first (cached layer if pom.xml unchanged)
RUN apk add --no-cache maven && \
    mvn dependency:go-offline -q

# Build the fat JAR, skip tests (tests run in CI pipeline separately)
RUN mvn package -DskipTests -q

# ─── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Create non-root user for security
RUN addgroup -S batchgroup && adduser -S batchuser -G batchgroup

# Copy the fat JAR
COPY --from=builder /workspace/target/batch-upload-*.jar app.jar

# The shared NFS/PVC will be mounted here by Kubernetes
RUN mkdir -p /data && chown batchuser:batchgroup /data

USER batchuser

EXPOSE 8080

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-XX:+UseG1GC", \
  "-XX:MaxGCPauseMillis=200", \
  "-jar", "app.jar"]
