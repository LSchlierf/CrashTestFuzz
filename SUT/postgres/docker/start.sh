CRASHCMD=${CRASHCMD:=""}

echo "#begin custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
echo "$CRASHCMD" >> /lazyfs/lazyfs/config/config.toml && \
echo "#end custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
cd /lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c /lazyfs/lazyfs/config/config.toml -m /var/lib/postgresql/data -r /tmp/lazyfs.root && \
sleep 3 && \
script -qfc "docker-entrypoint.sh postgres" /tmp/postgres.log & tail -f /dev/null
# echo "fsync = off" >> /usr/share/postgresql/postgresql.conf.sample && \