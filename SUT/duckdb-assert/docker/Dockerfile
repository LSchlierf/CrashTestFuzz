FROM ubuntu:22.04

# dependencies for lazyfs, duckdb server
RUN apt update -y && apt install -y git g++ cmake libfuse3-dev libfuse3-3 fuse3 python3.10 python3-pip ninja-build libssl-dev
RUN pip install fastapi uvicorn

# build duckdb with assertions
WORKDIR /
RUN git clone https://github.com/duckdb/duckdb
WORKDIR /duckdb
RUN git reset --hard 7c03946
RUN GEN=ninja make relassert
WORKDIR /duckdb/tools/pythonpkg
RUN pip install setuptools==61 pybind11
RUN python3.10 setup.py install

# allow fuse for lazyfs
RUN echo user_allow_other >> /etc/fuse.conf

# install lazyfs
WORKDIR /
RUN git clone https://github.com/dsrhaslab/lazyfs.git
WORKDIR /lazyfs/libs/libpcache
RUN ./build.sh
WORKDIR /lazyfs/lazyfs
RUN ./build.sh
WORKDIR /

# copy local lazyfs config into container
COPY ./config.toml /lazyfs/lazyfs/config/config.toml
RUN chmod a+r /lazyfs/lazyfs/config/config.toml
RUN chmod a+w /lazyfs/lazyfs/config/config.toml

RUN mkdir /tmp/lazyfs.root
RUN mkdir /var/db

# add user to container with same UID and GID as local user
ARG HOST_GID
ARG HOST_UID
ARG WAL_SYNC_METHOD

ENV HOST_GID=${HOST_GID}
ENV HOST_UID=${HOST_UID}
ENV WAL_SYNC_METHOD=${WAL_SYNC_METHOD}

RUN groupadd --gid $HOST_GID local
RUN useradd --uid $HOST_UID --gid $HOST_GID local

EXPOSE 5432

COPY ./start.sh /start.sh
RUN chmod 0755 /start.sh

COPY ./server.py /server.py
RUN chmod 0755 /server.py

RUN echo "pkill -15 python3.10" > /stop-duckdb-assert.sh
RUN chmod +x /stop-duckdb-assert.sh

RUN echo "/stop-duckdb-assert.sh && sleep 3 && /lazyfs/lazyfs/scripts/umount-lazyfs.sh -m /var/db && pkill tail" > /stop-all.sh
RUN chmod +x /stop-all.sh

RUN chown -R $HOST_UID:$HOST_GID /var/db
USER local

CMD /start.sh
