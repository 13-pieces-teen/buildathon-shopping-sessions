FROM rust:1.94-bookworm AS build
WORKDIR /build
COPY src/ ./src/
RUN g++ -std=c++20 -O3 -DNDEBUG -pthread -Wall -Wextra src/main.cpp src/ingest.cpp src/aggregate.cpp src/output.cpp -o /build/shopping-sessions

FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir pyarrow==22.0.0 && useradd --uid 1000 --no-create-home --home-dir /scratch app
WORKDIR /app
COPY --from=build /build/shopping-sessions /app/shopping-sessions
COPY write_parquet.py run.sh /app/
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["/app/run.sh"]
