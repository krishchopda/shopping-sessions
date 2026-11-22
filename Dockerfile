# Shopping Sessions: Somesh Agrawal's Rust pipeline, one static binary.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && strip target/x86_64-unknown-linux-gnu/release/sessions \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output
USER 1000:1000
CMD ["/sessions"]
