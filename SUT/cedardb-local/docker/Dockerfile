FROM ubuntu:25.10

USER root

# dependencies
RUN apt update && apt install -y git g++ cmake libfuse3-dev libfuse3-4 fuse3 lld llvm-21-dev clang-21 clang-format-21 clang-tidy-21 bison libboost-dev libboost-context-dev libboost-filesystem-dev libboost-system-dev libre2-dev libssl-dev libpq-dev liburing-dev zlib1g ccache curl mold

# allow fuse for lazyfs
RUN echo "user_allow_other" >> /etc/fuse.conf

# install lazyfs
WORKDIR /
RUN git clone https://github.com/dsrhaslab/lazyfs.git
RUN sed -i 's/MAX_ARGS = 10/MAX_ARGS = 20/g' lazyfs/lazyfs/src/main.cpp
#WORKDIR lazyfs
#RUN git checkout 9c5685e
WORKDIR /lazyfs/libs/libpcache
RUN ./build.sh
WORKDIR /lazyfs/lazyfs
RUN ./build.sh
WORKDIR /

# copy local lazyfs config into container
COPY ./config.toml /lazyfs/lazyfs/config/config.toml
RUN chmod a+r /lazyfs/lazyfs/config/config.toml
RUN chmod a+w /lazyfs/lazyfs/config/config.toml

RUN echo "pkill -15 cedardb" > /stop-cedardb.sh
RUN chmod +x /stop-cedardb.sh

RUN echo "/stop-cedardb.sh && sleep 3 && /lazyfs/lazyfs/scripts/umount-lazyfs.sh -m /var/db && pkill tail" > /stop-all.sh
RUN chmod +x /stop-all.sh


ARG HOST_GID
ARG HOST_UID
ARG WAL_SYNC_METHOD

ENV HOST_GID=${HOST_GID}
ENV HOST_UID=${HOST_UID}
ENV WAL_SYNC_METHOD=${WAL_SYNC_METHOD}
ENV CEDAR_PASSWORD=postgres
ENV ASYNCCOMMITS=off
ENV CEDARDB_DATA=/mnt/cedardb/data
ENV CEDARDB_PORT=5432
ENV CEDAR_USER=postgres
ENV CEDAR_PASSWORD=postgres
ENV CEDAR_DB=postgres

# if user with HOST_UID exists, change name to "local" else create with name "local"
RUN if [ $(getent passwd ${HOST_UID}) ]; then LOCALNAME=$(getent passwd ${HOST_UID} | cut -f 1 -d ":") && usermod -l local ${LOCALNAME} && groupmod -n local ${LOCALNAME} ; else groupadd --gid ${HOST_GID} local && useradd --uid ${HOST_UID} --gid ${HOST_GID} local ; fi

RUN groupadd fuse
RUN usermod -aG fuse local

RUN mkdir /tmp/lazyfs.root
RUN chown -R $HOST_UID:$HOST_GID /tmp/lazyfs.root

RUN mkdir /mnt/cedardb
RUN mkdir /mnt/cedardb/data
RUN mkdir /var/run/postgresql
RUN chown -R $HOST_UID:$HOST_GID /mnt/cedardb
RUN chown -R $HOST_UID:$HOST_GID /var/run/postgresql

# move the file or create a symlink
COPY ./cedardb /usr/local/bin/cedardb
COPY ./docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

COPY ./start.sh /start.sh
RUN chmod 0755 /start.sh

RUN chmod a+x /usr/local/bin/docker-entrypoint.sh
RUN chmod a+r /usr/local/bin/docker-entrypoint.sh
RUN chown $HOST_UID:$HOST_GID /usr/local/bin/docker-entrypoint.sh
RUN chown $HOST_UID:$HOST_GID /usr/local/bin/cedardb

# run cedardb as local user
USER local

ENTRYPOINT /start.sh
