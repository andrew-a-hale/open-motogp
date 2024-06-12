FROM python:3.11-bookworm as base

COPY . /app

RUN apt update \
    && apt install -y make \
    && wget https://sqlite.org/2024/sqlite-tools-linux-x64-3460000.zip \
    && unzip sqlite-tools-linux-x64-3460000.zip \
    && mv sqlite3 /bin/sqlite3 \
    && wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /bin/duckdb \
    && pip install -r /app/requirements.txt

RUN pip install wheel && pip wheel /app --wheel-dir=/svc/wheels

FROM python:3.11-bookworm
COPY --from=base /svc /svc
WORKDIR /svc
RUN pip install --no-index --find-links=/svc/wheels -r requirements.txt