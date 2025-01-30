CRASHCMD=${CRASHCMD:=""}

echo "#begin custom cmd" >> /lazyfs/lazyfs/config/default.toml && \
echo "$CRASHCMD" >> /lazyfs/lazyfs/config/default.toml && \
echo "#end custom cmd" >> /lazyfs/lazyfs/config/default.toml && \
cd /lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c /lazyfs/lazyfs/config/default.toml -m /var/db -r /tmp/lazyfs.root && \
sleep 1 && \
script -qfc /docker-entrypoint.sh /tmp/umbra.log & tail -f /dev/null