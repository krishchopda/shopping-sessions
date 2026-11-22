# Shopping Sessions: Somesh Agrawal's Rust pipeline, one static binary.
# Profile-guided build: train on small datasets from the official generator
# (tools/generator, seeds unrelated to any scoring data), then rebuild with the
# profile. If any training step fails, fall back to the plain build.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tools/generator ./tools/generator
ENV BASE_FLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
ENV T=x86_64-unknown-linux-gnu
RUN set -e; \
    if ( rustup component add llvm-tools \
         && cargo build --release --manifest-path tools/generator/Cargo.toml \
         && tools/generator/target/release/generate --seed 7 --events 4000000 --clicks 1000000 --output /tmp/tr7 \
         && tools/generator/target/release/generate --seed 8 --events 1000000 --clicks 250000 --output /tmp/tr8 \
         && RUSTFLAGS="$BASE_FLAGS -Cprofile-generate=/tmp/pgo" cargo build --release --locked --target $T --target-dir /tmp/gen \
         && DATA_DIR=/tmp/tr7 OUTPUT_DIR=/tmp/o7 NCPU=8 /tmp/gen/$T/release/sessions \
         && DATA_DIR=/tmp/tr8 OUTPUT_DIR=/tmp/o8 NCPU=8 /tmp/gen/$T/release/sessions \
         && "$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)" merge -o /tmp/pgo.profdata /tmp/pgo ); \
    then echo "PGO build"; RUSTFLAGS="$BASE_FLAGS -Cprofile-use=/tmp/pgo.profdata" cargo build --release --locked --target $T; \
    else echo "plain build"; RUSTFLAGS="$BASE_FLAGS" cargo build --release --locked --target $T; \
    fi; \
    strip target/$T/release/sessions && cp target/$T/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
ENV DATA_DIR=/data OUTPUT_DIR=/output AVX512=1
USER 1000:1000
CMD ["/sessions"]
