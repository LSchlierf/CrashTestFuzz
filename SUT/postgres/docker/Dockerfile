FROM postgres:latest

USER root

# dependencies for lazyfs
RUN apt update -y && apt install -y git g++ cmake libfuse3-dev libfuse3-3 fuse3

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
COPY ./config.toml /lazyfs/lazyfs/config/default.toml
RUN chmod a+r /lazyfs/lazyfs/config/default.toml
RUN chmod a+w /lazyfs/lazyfs/config/default.toml

# run pg on lazyfs by mounting lazyfs to /var/db
COPY ./start.sh /start.sh
# RUN echo "cd /lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c /lazyfs/lazyfs/config/default.toml -m /var/db -r /tmp/lazyfs.root && sleep 1 && script -qfc /docker-entrypoint.sh /tmp/pg.log & tail -f /dev/null" > /start.sh
RUN chmod 0777 /start.sh

RUN echo "pkill -15 postgres" > /stop-postgres.sh
RUN chmod +x /stop-postgres.sh

RUN echo "/stop-postgres.sh && sleep 3 && /lazyfs/lazyfs/scripts/umount-lazyfs.sh -m /var/lib/postgresql/data && pkill tail" > /stop-all.sh
RUN chmod +x /stop-all.sh

RUN mkdir /tmp/lazyfs.root
RUN chown -R $HOST_UID:$HOST_GID /tmp/lazyfs.root
RUN chmod 0700 /tmp/lazyfs.root

# add user to container with same UID and GID as local user
ARG HOST_GID
ARG HOST_UID
ARG WAL_SYNC_METHOD

ENV HOST_GID=${HOST_GID}
ENV HOST_UID=${HOST_UID}
ENV WAL_SYNC_METHOD=${WAL_SYNC_METHOD}
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_HOST_AUTH_METHOD=trust
ENV POSTGRES_INITDB_ARGS="--set fsync=off --auth=trust"

RUN groupadd --gid $HOST_GID local
RUN useradd --uid $HOST_UID --gid $HOST_GID local

# COPY ./docker-entrypoint.sh /docker-entrypoint.sh
# RUN chmod +x /docker-entrypoint.sh
# RUN chown -R $HOST_UID:$HOST_GID /docker-entrypoint.sh

# run docker as local user
RUN chown -R $HOST_UID:$HOST_GID /var/lib/postgresql
RUN chown -R $HOST_UID:$HOST_GID /usr/local/bin
# RUN echo "fsync = off" >> /var/lib/postgresql/data/postgresql.conf
USER local

# ENTRYPOINT [ "sh", "/start.sh" ]
CMD /start.sh
