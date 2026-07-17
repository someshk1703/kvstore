# ────────────────────────────────────────────────────────────────────────────
# Stage 1 — Build
# ────────────────────────────────────────────────────────────────────────────
# Use the official Maven + JDK 21 image to compile and package the fat JAR.
# Doing the build inside Docker ensures reproducibility — no "works on my
# machine" with a different Maven version.
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Copy dependency metadata first so Docker can cache the dependency-download
# layer separately from source changes. This makes subsequent builds faster.
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Now copy source and build. -DskipTests is intentional: tests run in CI, not
# in the Dockerfile. The fat JAR produced by spring-boot-maven-plugin is what
# we deploy.
COPY src/ src/
RUN mvn clean package -DskipTests -q

# ────────────────────────────────────────────────────────────────────────────
# Stage 2 — Runtime
# ────────────────────────────────────────────────────────────────────────────
# Use a slim JRE-only image. We don't need Maven or a full JDK at runtime.
FROM eclipse-temurin:21-jre-alpine

# Non-root user — security best practice (OWASP: run as least privilege)
RUN addgroup -S kvstore && adduser -S kvstore -G kvstore
USER kvstore

WORKDIR /app

# Copy only the fat JAR from the build stage
COPY --from=build /build/target/kvstore-0.1.0-SNAPSHOT.jar kvstore.jar

# ── Ports ────────────────────────────────────────────────────────────────────
# 6379  — TCP protocol (RESP-lite)
# 6380  — Replication port (primary-mode)
# 8080  — HTTP API (Spring Boot, only when --http is passed)
EXPOSE 6379 6380 8080

# ── Healthcheck ──────────────────────────────────────────────────────────────
# Sends a PING over TCP and checks for "+PONG". Requires nc (netcat).
# Alpine ships busybox which includes nc.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD printf 'PING\r\n' | nc -w 1 localhost 6379 | grep -q '+PONG' || exit 1

# ── Default entrypoint ───────────────────────────────────────────────────────
# Arguments can be overridden in docker-compose.yml per service.
ENTRYPOINT ["java", "-jar", "kvstore.jar"]
CMD ["--http"]
