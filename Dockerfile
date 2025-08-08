FROM rustlang/rust:nightly AS builder
WORKDIR /usr/src/solana-usdc-indexer
COPY . .
RUN cargo --version
RUN rm -rf /usr/local/cargo/registry
RUN cargo update
RUN cargo build --release

FROM rustlang/rust:nightly-slim
WORKDIR /usr/src/solana-usdc-indexer
COPY --from=builder /usr/src/solana-usdc-indexer/target/release/solana-usdc-indexer .
EXPOSE 8080
CMD ["./solana-usdc-indexer"]