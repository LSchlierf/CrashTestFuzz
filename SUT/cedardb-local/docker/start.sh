CRASHCMD=${CRASHCMD:=""}

echo "#begin custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
echo "$CRASHCMD" >> /lazyfs/lazyfs/config/config.toml && \
echo "#end custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
cd /lazyfs/lazyfs && ./build/lazyfs /mnt/cedardb/data --config-path /lazyfs/lazyfs/config/config.toml -o allow_other -o modules=subdir -o subdir=/tmp/lazyfs.root && \
sleep 5 && \
script -qfc /usr/local/bin/docker-entrypoint.sh /tmp/cedardb.log & tail -f /dev/null
#cd /lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c /lazyfs/lazyfs/config/config.toml -m /mnt/cedardb/data -r /tmp/lazyfs.root && \