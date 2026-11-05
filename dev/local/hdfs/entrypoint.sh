#!/usr/bin/env bash
set -euo pipefail

export PATH="${HADOOP_HOME:-/opt/hadoop}/bin:${PATH}"
mkdir -p "${HADOOP_LOG_DIR}" "${HADOOP_PID_DIR}"

case "${1:-}" in
  init)
    state=/storage/namenode
    mkdir -p "$state"
    if [ -f "$state/current/VERSION" ]; then
      grep -qx 'clusterID=CID-hmdp-lab-v1' "$state/current/VERSION" || {
        echo 'Foreign cluster metadata; refusing to reformat.' >&2
        exit 2
      }
      echo 'Existing NameNode preserved; no format performed.'
      exit 0
    fi
    if [ -n "$(find "$state" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
      echo 'Nonempty NameNode directory without VERSION; inspect it manually.' >&2
      exit 2
    fi
    exec hdfs namenode -format -clusterid CID-hmdp-lab-v1 -nonInteractive
    ;;
  namenode)
    test -f /storage/namenode/current/VERSION || {
      echo 'NameNode is not initialized; run hdfs_control.py init explicitly.' >&2
      exit 2
    }
    grep -qx 'clusterID=CID-hmdp-lab-v1' /storage/namenode/current/VERSION || {
      echo 'Foreign cluster metadata; refusing to start.' >&2
      exit 2
    }
    exec hdfs namenode
    ;;
  datanode)
    mkdir -p /storage/datanode
    exec hdfs datanode
    ;;
  *)
    echo 'Expected init, namenode or datanode.' >&2
    exit 2
    ;;
esac
