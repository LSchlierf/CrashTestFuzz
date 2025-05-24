FROM cedardb/cedardb:v2025-05-14
# https://cedardb.com/docs/installation/

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
COPY ./config.toml /lazyfs/lazyfs/config/config.toml
RUN chmod a+r /lazyfs/lazyfs/config/config.toml
RUN chmod a+w /lazyfs/lazyfs/config/config.toml

COPY ./start.sh /start.sh
RUN chmod 0755 /start.sh

RUN echo "pkill -15 cedardb" > /stop-cedardb.sh
RUN chmod +x /stop-cedardb.sh

RUN echo "/stop-cedardb.sh && sleep 3 && /lazyfs/lazyfs/scripts/umount-lazyfs.sh -m /var/db && pkill tail" > /stop-all.sh
RUN chmod +x /stop-all.sh

RUN mkdir /tmp/lazyfs.root

ARG HOST_GID
ARG HOST_UID
ARG WAL_SYNC_METHOD

ENV HOST_GID=${HOST_GID}
ENV HOST_UID=${HOST_UID}
ENV WAL_SYNC_METHOD=${WAL_SYNC_METHOD}
ENV CEDAR_PASSWORD=postgres
ENV ASYNCCOMMITS=off

# if user with HOST_UID exists, change name to "local" else create with name "local"
RUN if [ $(getent passwd ${HOST_UID}) ]; then LOCALNAME=$(getent passwd ${HOST_UID} | cut -f 1 -d ":") && usermod -l local ${LOCALNAME} && groupmod -n local ${LOCALNAME} ; else groupadd --gid ${HOST_GID} local && useradd --uid ${HOST_UID} --gid ${HOST_GID} local ; fi

COPY ./docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod a+x /docker-entrypoint.sh
RUN chmod a+r /docker-entrypoint.sh
RUN chown -R $HOST_UID:$HOST_GID /docker-entrypoint.sh

# run cedardb as local user
RUN chown -R $HOST_UID:$HOST_GID /var/lib/cedardb
RUN chown -R $HOST_UID:$HOST_GID /var/run/postgresql
USER local

ENTRYPOINT /start.sh
