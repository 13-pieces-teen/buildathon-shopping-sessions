FROM python:3.12-slim-bookworm AS runtime
RUN pip install --no-cache-dir pyarrow==22.0.0 && useradd --uid 1000 --no-create-home --home-dir /scratch app

FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
COPY --from=runtime /usr/local/lib/python3.12/site-packages/pyarrow/ /arrow/
WORKDIR /build
COPY src/ ./src/
RUN g++ -std=c++20 -O3 -DNDEBUG -pthread -Wall -Wextra -I/arrow/include \
    src/main.cpp src/ingest.cpp src/aggregate.cpp src/output_arrow.cpp \
    -L/arrow -Wl,-rpath,/usr/local/lib/python3.12/site-packages/pyarrow \
    -Wl,-rpath-link,/arrow -l:libparquet.so.2200 -l:libarrow.so.2200 \
    -o /build/shopping-sessions

FROM runtime
WORKDIR /app
COPY --from=build /build/shopping-sessions /app/shopping-sessions
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["/app/run.sh"]
