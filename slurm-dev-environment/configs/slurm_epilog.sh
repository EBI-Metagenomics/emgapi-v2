#!/bin/bash


# Remove the /nfs/public and /nfs/ftp "mounts" if they exists
# This would be at the end of a datamover job.
# Note that we can't check the slurm partition in this epilog, because slurm doesn't populate the env var
# (at least in older slurm versions...)
NFS_TARGET="/nfs/public"
FTP_TARGET="/nfs/ftp"

if [ -e "$NFS_TARGET" ]; then
    rm -rf "$NFS_TARGET"
fi

if [ -e "$FTP_TARGET" ]; then
    rm -rf "$FTP_TARGET"
fi

exit 0
