CRASHCMD=${CRASHCMD:=""}

echo "#begin custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
echo "$CRASHCMD" >> /lazyfs/lazyfs/config/config.toml && \
echo "#end custom cmd" >> /lazyfs/lazyfs/config/config.toml && \
cd /lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c /lazyfs/lazyfs/config/config.toml -m /var/db -r /tmp/lazyfs.root && \
sleep 3 && \
script -qfc /server.py /tmp/sqlite.log & tail -f /dev/null