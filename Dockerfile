# Shopping Sessions: Somesh Agrawal's C++ pipeline (cpp/), general path only,
# built with a recorded GCC 13 profile (cpp/main.gcda, from a run on the
# generator's 1M-event dev set and a 200k-event set).
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY cpp/ ./
RUN g++ -O3 -std=c++20 -march=x86-64-v3 -pthread -fprofile-use -fprofile-correction -Wno-missing-profile -c main.cpp -o main.o \
 && g++ -static -pthread main.o -o /solver

FROM debian:bookworm-slim
COPY --from=build /solver /app/solver
USER 1000:1000
CMD ["/app/solver"]
