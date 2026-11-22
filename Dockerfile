# Shopping Sessions: Somesh Agrawal's C++ pipeline (cpp/), general path only.
FROM gcc:14 AS build
WORKDIR /src
COPY cpp/ ./cpp/
RUN g++ -O3 -std=c++20 -march=cascadelake -static -pthread cpp/main.cpp -o /solver

FROM debian:bookworm-slim
COPY --from=build /solver /app/solver
USER 1000:1000
CMD ["/app/solver"]
