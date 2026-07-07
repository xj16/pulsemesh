# --- Build stage: compile an OTP release with rebar3 -----------------------
FROM erlang:26 AS build
WORKDIR /app

# Cache deps first.
COPY rebar.config rebar.lock* ./
RUN rebar3 get-deps || true

COPY src ./src
COPY config ./config
RUN rebar3 as prod release

# --- Runtime stage: minimal Debian with the self-contained release ---------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends libncurses6 libstdc++6 openssl wget \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/_build/prod/rel/pulsemesh_fabric ./

# Enable ${VAR} substitution in sys.config.src at release start, and provide
# defaults so the container is runnable without an external compose file.
ENV RELX_REPLACE_OS_VARS=true \
    PULSEMESH_FABRIC_PORT=8090 \
    PULSEMESH_AMQP_URI="amqp://guest:guest@rabbitmq:5672" \
    PULSEMESH_AMQP_EXCHANGE="pulsemesh.events" \
    PULSEMESH_AMQP_QUEUE="pulsemesh.fabric" \
    PULSEMESH_CQ_URL="http://command-query:8080"
EXPOSE 8090

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
  CMD ["sh", "-c", "wget -qO- http://localhost:8090/healthz || exit 1"]

# `foreground` runs the release in the container's main process.
ENTRYPOINT ["/app/bin/pulsemesh_fabric", "foreground"]
