#!/bin/bash

PARTITION_NAME=$SLURM_JOB_PARTITION

# If this is a datamover job (datamover partition) then fake the mounting of additional nfs filesystems
# In reality we are just making symlinks, because this is a dev env after all.
HIDDEN_NFS_SOURCE="/publicnfs"
NFS_TARGET="/nfs/public"

HIDDEN_FTP_SOURCE="/publicftp"
FTP_TARGET="/nfs/ftp"

if [ "$PARTITION_NAME" == "datamover" ]; then
    # NFS PUBLIC
    if [ -e "$NFS_TARGET" ]; then
        rm -rf "$NFS_TARGET"
    fi
    ln -s "$HIDDEN_NFS_SOURCE" "$NFS_TARGET"


    # FTP
    if [ -e "$FTP_TARGET" ]; then
        rm -rf "$FTP_TARGET"
    fi
    ln -s "$HIDDEN_FTP_SOURCE" "$FTP_TARGET"

    sleep 5
fi

exit 0
