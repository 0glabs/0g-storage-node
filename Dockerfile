# ---------- Dockerfile ----------
FROM rust AS builder

# 0) Install build deps (same as you had)
RUN apt-get update && \
    apt-get install -y clang cmake build-essential pkg-config libssl-dev

# 1) Copy sources and build the binary
WORKDIR /app
COPY . .
RUN cargo build --release

# 2) Keep the binary on $PATH (optional convenience)
RUN install -Dm755 target/release/zgs_node /usr/local/bin/zgs_node


FROM debian:bookworm-slim

# 3) Install required runtime libs
RUN apt-get update && \
    apt-get install -y libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 4) Copy binary from builder
COPY --from=builder /usr/local/bin/zgs_node /usr/local/bin/zgs_node

# 5) Persist chain data
VOLUME ["/data"]

###############################################################################
# 6) Runtime flags – grab everything from env vars that you’ll pass with
#    `docker run -e …`.  Shell-form CMD lets us interpolate ${…} at start-time.
###############################################################################
CMD zgs_node \
    --config run/config-testnet-turbo.toml \
    --log-config-file run/log_config \
    --miner-key "${STORAGE_MINER_PRIVATE_KEY:?missing STORAGE_MINER_PRIVATE_KEY}" \
    --blockchain-rpc-endpoint "${STORAGE_BLOCKCHAIN_RPC_ENDPOINT:?missing STORAGE_BLOCKCHAIN_RPC_ENDPOINT}" \
    --network-enr-address "${STORAGE_ENR_ADDRESS:?missing STORAGE_ENR_ADDRESS}" \
    --db-max-num-chunks "${STORAGE_DB_MAX_NUM_SECTORS:-8000000000}"

