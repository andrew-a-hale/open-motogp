FROM python:3.12-bullseye

# install make
RUN apt install -y make 

# install duckdb
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && mv duckdb /bin/duckdb # buildkit 60.8MB buildkit.dockerfile.v0

# install gum
RUN wget https://github.com/charmbracelet/gum/releases/download/v0.14.1/gum-0.14.1.tar.gz \
    && tar -xvf gum-0.14.1.tar.gz \
    && mv gum-0.14.1.tar.gz /bin/gum

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["./o-mgp.sh"]
