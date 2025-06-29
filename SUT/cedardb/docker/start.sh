CRASHCMD=${CRASHCMD:=""}

echo "#begin custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
echo "$CRASHCMD" >> /lazyfs/lazyfs/config/config.toml && \
echo "#end custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
cd /lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c /lazyfs/lazyfs/config/config.toml -m /var/lib/cedardb/data -r /tmp/lazyfs.root && \
sleep 3 && \
script -qfc /usr/local/bin/docker-entrypoint.sh /tmp/cedardb.log & tail -f /dev/null