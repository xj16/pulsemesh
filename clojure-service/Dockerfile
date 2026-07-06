# --- Build stage: compile the AOT uberjar with the Clojure CLI tools --------
FROM clojure:temurin-21-tools-deps-bookworm AS build
WORKDIR /app

# Cache deps first for faster rebuilds.
COPY deps.edn build.clj ./
RUN clojure -P && clojure -A:build -P || true

COPY src ./src
COPY resources ./resources
RUN clojure -T:build uber

# --- Runtime stage: slim JRE with just the jar -----------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app
COPY --from=build /app/target/pulsemesh.jar ./pulsemesh.jar

ENV PULSEMESH_HTTP_PORT=8080
EXPOSE 8080

# A cheap liveness probe against the built-in /health endpoint.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
  CMD ["sh", "-c", "wget -qO- http://localhost:${PULSEMESH_HTTP_PORT}/health || exit 1"]

ENTRYPOINT ["java", "-jar", "pulsemesh.jar"]
