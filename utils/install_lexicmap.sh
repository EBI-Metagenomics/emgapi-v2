#!/bin/sh
set -eu
version="${LEXICMAP_VERSION:-0.9.0}"
case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo 'Unsupported architecture' >&2; exit 1 ;;
esac
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
curl -fsSL --retry 3 "https://github.com/shenwei356/LexicMap/releases/download/v${version}/lexicmap_linux_${arch}.tar.gz" -o "$workdir/lexicmap.tar.gz"
tar -xzf "$workdir/lexicmap.tar.gz" -C "$workdir"
install -m 0755 "$workdir/lexicmap" /usr/local/bin/lexicmap
