FROM python:3.11-bookworm

RUN apt update
RUN apt install -y make 

# install sqlite3 latest
RUN wget https://sqlite.org/2024/sqlite-tools-linux-x64-3460000.zip \
    && unzip sqlite-tools-linux-x64-3460000.zip \
    && mv sqlite3 /bin/sqlite3

# install duckdb
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /bin/duckdb

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt

ENTRYPOINT [ "make", "test" ]
