# Shopping Sessions: static-ish Rust binary, tiny runtime image.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/solver /app/solver
USER 1000:1000
CMD ["/app/solver"]
