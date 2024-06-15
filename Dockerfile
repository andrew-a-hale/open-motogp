FROM python:3.11-alpine as builder

COPY . /app
WORKDIR /app

RUN apk add make wget unzip \
    && wget https://sqlite.org/2024/sqlite-tools-linux-x64-3460000.zip \
    && unzip sqlite-tools-linux-x64-3460000.zip \
    && wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && python -m pip install wheel \
    && python -m pip wheel --no-cache-dir --wheel-dir=/root/wheels -r requirements.txt \
    && python -m pip wheel --no-cache-dir --wheel-dir=/root/wheels .

FROM python:3.11-alpine as base

COPY --from=builder /root/wheels /root/wheels
COPY --from=builder /app/src /app/src
COPY --from=builder /app/Makefile /app/Makefile
COPY --from=builder /app/pyproject.toml /app/pyproject.toml

COPY --from=builder /app/duckdb /usr/bin/duckdb
COPY --from=builder /app/sqlite3 /usr/bin/sqlite3
COPY --from=builder /usr/bin/make /usr/bin/make

RUN python -m pip install --no-cache --no-index /root/wheels/* 
RUN rm -rf /root/wheels

WORKDIR /app

RUN make test