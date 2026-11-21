# Shopping Sessions: Rust binary built with profile-guided optimisation.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
# PGO: build an instrumented binary, run it on generated training data
# (src/bin/pgogen.rs, our own generator; no data is checked in), merge the
# profile, rebuild with it. Any failure falls back to a plain release build,
# so the image always builds. PGO changes code layout only, never results.
RUN cargo build --release --locked --bin pgogen \
 && ( rustup component add llvm-tools \
   && PD="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/llvm-profdata | head -1)" \
   && RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-generate=/tmp/pgo" CARGO_PROFILE_RELEASE_LTO=false \
        cargo build --release --locked --bin solver --target-dir /tmp/inst \
   && /src/target/release/pgogen /tmp/train 2000000 \
   && X_PGO_TRAIN=1 DATA_DIR=/tmp/train OUTPUT_DIR=/tmp/out NCPU=4 /tmp/inst/release/solver \
   && "$PD" merge -o /tmp/pgo.profdata /tmp/pgo \
   && RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-use=/tmp/pgo.profdata -C llvm-args=-pgo-warn-mismatch=false" \
        cargo build --release --locked --bin solver \
   && echo "PGO build OK" ) \
 || cargo build --release --locked --bin solver

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/solver /app/solver
ENV X_PROBE=1
USER 1000:1000
CMD ["/app/solver"]
