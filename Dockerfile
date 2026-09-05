FROM postgres:15

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-15 \
    gcc \
    make \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch 1.4.2 https://github.com/HypoPG/hypopg.git /hypopg \
    && make -C /hypopg PG_CONFIG=/usr/bin/pg_config \
    && make -C /hypopg install PG_CONFIG=/usr/bin/pg_config

COPY docker/init/*.sql /docker-entrypoint-initdb.d/
