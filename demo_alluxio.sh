function doas() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function doas, expecting 2"
    exit 2
  fi
  local user="$1"
  local cmd="$2"

  sudo runuser -l "${user}" -c "${cmd}"
}

function get_hdfs_namenodes() {
    hdfs getconf -namenodes
}

function show_mount_hdfs() {
    if [ "$#" -ne 1 ]; then
        echo "Pass hdfs namenode..."
        return 1
    fi

    echo "alluxio fs mount MOUNT_POINT \"hdfs://${1}:8020/\""
}

function show_commands_hdfs() {
    echo "run \"prepare_tpcds_data_hdfs\" on \"on_prem\" to prepare data"

    echo "alluxio fs checksum   /hdfs_comp/tmp/foo"
    echo "alluxio fs ls         /hdfs_comp/tmp"
    echo "alluxio fs mkdir      /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs ls         /hdfs_comp/tmp"
    echo "alluxio fs chmod 666  /hdfs_comp/tmp/foo"
    echo "alluxio fs ls         /hdfs_comp/tmp"
    echo "alluxio fs chown alluxio:alluxio /hdfs_comp/tmp/foo"
    echo "alluxio fs ls         /hdfs_comp/tmp" 
    echo "alluxio fs du -h      /hdfs_comp/tmp"
    echo "alluxio fs copyFromLocal /tmp/foo /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs ls         /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs rm -R      /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs ls         /hdfs_comp/tmp" 
}

function show_metadata_sync_hdfs() {
    echo "hadoop fs -mkdir /tmp/metadata-sync-test"

    echo "[active sync]"
    echo "alluxio fs ls /hdfs_comp/tmp/metadata-sync-test"
    echo "alluxio fs getSyncPathList"
    echo "hadoop fs -copyFromLocal /tmp/foo /tmp/metadata-sync-test/foo1"
    echo "alluxio fs ls /hdfs_comp/tmp/metadata-sync-test"

    echo "[manual sync]"
    echo "alluxio fs stopSync /"
    echo "alluxio fs getSyncPathList"
    echo "hadoop fs -copyFromLocal /tmp/foo /tmp/metadata-sync-test/foo2"
    echo "alluxio fs ls /hdfs_comp/tmp/metadata-sync-test"
    echo "alluxio fs -Dalluxio.user.file.metadata.sync.interval=0 ls /hdfs_comp/tmp/metadata-sync-test"

    echo "[periodical sync]"
    echo "alluxio fsadmin pathConf add --property alluxio.user.file.metadata.sync.interval=5s /hdfs_comp/tmp/metadata-sync-test"
    echo "hadoop fs -copyFromLocal /tmp/foo /tmp/metadata-sync-test/foo3"
    echo "alluxio fs ls /hdfs_comp/tmp/metadata-sync-test"

    echo "[turn on active sync]"
    echo "alluxio fs startSync /"
}

function show_read_hdfs() {
    echo "hadoop fs -mkdir /tmp/read-test"
    echo "hadoop fs -copyFromLocal /tmp/foo /tmp/read-test/foo.cache"
    echo "hadoop fs -copyFromLocal /tmp/foo /tmp/read-test/foo.no_cache"

    echo "alluxio fs ls /hdfs_comp/tmp/read-test"
    echo "alluxio fs cat /hdfs_comp/tmp/read-test/foo.cache"
    echo "alluxio fs -Dalluxio.user.file.readtype.default=NO_CACHE cat /hdfs_comp/tmp/read-test/foo.no_cache"
    echo "alluxio fs ls /hdfs_comp/tmp/read-test"
}

function show_write_hdfs() {
    echo "hadoop fs -mkdir /tmp/write-test"
    echo "alluxio fs ls /hdfs_comp/tmp/write-test"

    echo "alluxio fs -Dalluxio.user.file.writetype.default=MUST_CACHE copyFromLocal /tmp/foo /hdfs_comp/tmp/write-test/foo.must_cache"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH copyFromLocal /tmp/foo /hdfs_comp/tmp/write-test/foo.cache_through"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=ASYNC_THROUGH copyFromLocal /tmp/foo /hdfs_comp/tmp/write-test/foo.async_through"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=THROUGH copyFromLocal /tmp/foo /hdfs_comp/tmp/write-test/foo.through"
    echo "alluxio fs ls /hdfs_comp/tmp/write-test"
}

function show_stat_hdfs() {
    echo "alluxio fs stat /hdfs_comp/tmp/write-test/foo.must_cache"
}

function show_mount_s3() {
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/foo"
    echo "alluxio fs mount /s3 s3://alluxio.saiguang.test/demo/"
}

function show_commands_s3() {
    echo "alluxio fs checksum   /s3/tmp/foo"
    echo "alluxio fs ls         /s3/tmp"
    echo "alluxio fs mkdir      /s3/tmp/test-mkdir"
    echo "alluxio fs ls         /s3/tmp"
    echo "alluxio fs chmod 666  /s3/tmp/foo"
    echo "alluxio fs ls         /s3/tmp"
    echo "alluxio fs chown alluxio:alluxio /s3/tmp/foo"
    echo "alluxio fs ls         /s3/tmp" 
    echo "alluxio fs du -h      /s3/tmp"
    echo "alluxio fs copyFromLocal /tmp/foo /s3/tmp/test-mkdir"
    echo "alluxio fs ls         /s3/tmp/test-mkdir"
    echo "alluxio fs rm -R      /s3/tmp/test-mkdir"
    echo "alluxio fs ls         /s3/tmp" 

    echo "[reset]"
    echo "alluxio fs chown hadoop:hadoop /s3/tmp/foo"
    echo "alluxio fs chmod 600  /s3/tmp/foo"
}

function show_metadata_sync_s3() {
    echo "[manual sync]"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/metadata-sync-test/foo1"
    echo "alluxio fs ls /s3/tmp/metadata-sync-test"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/metadata-sync-test/foo2"
    echo "alluxio fs ls /s3/tmp/metadata-sync-test"
    echo "alluxio fs -Dalluxio.user.file.metadata.sync.interval=0 ls /s3/tmp/metadata-sync-test"

    echo "[periodical sync]"
    echo "alluxio fsadmin pathConf add --property alluxio.user.file.metadata.sync.interval=5s /s3/tmp/metadata-sync-test"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/metadata-sync-test/foo3"
    echo "alluxio fs ls /s3/tmp/metadata-sync-test"
}

function show_read_s3() {
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/read-test/foo.cache"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/read-test/foo.no_cache"

    echo "alluxio fs ls /s3/tmp/read-test"
    echo "alluxio fs cat /s3/tmp/read-test/foo.cache"
    echo "alluxio fs -Dalluxio.user.file.readtype.default=NO_CACHE cat /s3/tmp/read-test/foo.no_cache"
    echo "alluxio fs ls /hdfs_comp/tmp/read-test"
}

function show_write_hdfs() {
    echo "alluxio fs mkdir /s3/tmp/write-test"

    echo "alluxio fs -Dalluxio.user.file.writetype.default=MUST_CACHE copyFromLocal /tmp/foo /s3/tmp/write-test/foo.must_cache"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH copyFromLocal /tmp/foo /s3/tmp/write-test/foo.cache_through"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=ASYNC_THROUGH copyFromLocal /tmp/foo /s3/tmp/write-test/foo.async_through"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=THROUGH copyFromLocal /tmp/foo /s3/tmp/write-test/foo.through"

    echo "alluxio fs ls /hdfs_comp/tmp/write-test"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/tmp/write-test/"
}

function show_stat_s3() {
    echo "alluxio fs stat /s3/tmp/write-test/foo.must_cache"
}

function setup_fuse() {
    local -r is_master=$(jq '.isMaster' /mnt/var/lib/info/instance.json)

    if [[ "$is_master" == "true" ]]; then
        echo "run this on worker node"
        return 1
    fi

    doas alluxio "alluxio-stop.sh worker"

    echo "user_allow_other" | sudo tee /etc/fuse.conf  # config for FUSE

    sudo mkdir -p /mnt/alluxio-fuse
    sudo chmod 755 /mnt/alluxio-fuse
    sudo chown alluxio /mnt/alluxio-fuse
    
    alluxio-mount.sh SudoMount local  # sudo mount ramdisk
    doas alluxio "alluxio-start.sh worker"
}

function show_use_fuse() {
    echo "ls /mnt/alluxio-fuse"
    echo "ls /mnt/alluxio-fuse/s3"
    echo "ls /mnt/alluxio-fuse/hdfs_comp"

    echo "cp /tmp/foo /mnt/alluxio-fuse/s3/tmp/foo.posix"
    echo "cp /tmp/foo /mnt/alluxio-fuse/hdfs_comp/tmp/foo.posix"

    echo "ls /mnt/alluxio-fuse/s3/tmp/"
    echo "ls /mnt/alluxio-fuse/hdfs_comp/tmp/"

    echo "cat /mnt/alluxio-fuse/s3/tmp/foo.posix"
    echo "cat /tmp/foo /mnt/alluxio-fuse/hdfs_comp/tmp/foo.posix"
}

function show_unified_namespace() {
    echo "alluxio fs ls /hdfs_comp/tmp"
    echo "alluxio fs ls /s3/tmp"

    echo "alluxio fs cat /hdfs_comp/tmp/read-test/foo.cache"
    echo "alluxio fs ls /s3/tmp/read-test/foo.cache"
}

function show_cache_control() {
    echo "[load & free]"
    echo "alluxio fs mkdir /hdfs_comp/tmp/cache-test"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=ASYNC_THROUGH copyFromLocal /tmp/foo /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "alluxio fs free /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "alluxio fs load /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"

    echo "[pin & unpin]"
    echo "alluxio fs pin /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "alluxio fs free /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "alluxio fs unpin /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "alluxio fs free /hdfs_comp/tmp/cache-test/foo.async_through"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"

    echo "[persist]"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=MUST_CACHE copyFromLocal /tmp/foo /hdfs_comp/tmp/cache-test/foo.must_cache"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "hadoop fs -ls /tmp/cache-test"
    echo "alluxio fs persist /hdfs_comp/tmp/cache-test/foo.must_cache"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "hadoop fs -ls /tmp/cache-test"

    echo "[ttl]"
    echo "set \"alluxio.master.ttl.checker.interval=7s\" and run \"alluxio-start.sh master\""
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "alluxio fs setTtl --action free /hdfs_comp/tmp/cache-test/foo.must_cache 5s"
    echo "alluxio fs ls /hdfs_comp/tmp/cache-test"
    echo "unset \"alluxio.master.ttl.checker.interval=7s\" and run \"alluxio-start.sh master\""
}

function show_spark() {
    echo "alluxio fs rm -R /s3/tmp/*"
    echo "alluxio fs rm -R /hdfs_comp/tmp/*"
    echo "alluxio fs free /"

    echo "alluxio fs mount /s3-tpcds s3://alluxio.saiguang.test/tpcds/parquet/scale100/"
    echo "spark-shell"
    echo "spark.read.parquet(\"alluxio://$(get_hdfs_namenodes):19998/s3-tpcds/customer\").count"

    echo "alluxio fsadmin report"
    echo "alluxio fs ls /s3-tpcds/customer"
}

function show_presto() {
    echo "run \"prepare_tpcds_data_hdfs\" on \"on_prem\" to prepare data"
    echo "run \"download_tpcds_queries\" on \"compute\" to prepare queries"

    echo "alluxio fs free /"
    echo "hive -e \"show tables;\" # on_prem"
    echo "presto-cli --catalog onprem --schema default < ./queries/q44.sql # compute"
}

function show_transparent_uri() {

}

function show_ha_master() {
    echo "[on master /tmp/alluxio]"

    echo "bin/alluxio fs masterInfo"
    echo "bin/alluxio runTest"

    echo "jps | grep AlluxioMaster ; MASTER_PROC=$(jps | grep AlluxioMaster | awk '{ print $1}')"
    echo "bin/alluxio runTest & sleep 3; kill -9 \$MASTER_PROC ; fg %1"
    echo "bin/alluxio fs masterInfo"

    echo "bin/alluxio-start.sh master"
    echo "bin/alluxio fs masterInfo"
}

function show_ha_worker() {
    echo "[on worker /tmp/alluxio]"

    echo "head -c 10G </dev/urandom > /tmp/random.10G.touch"
    echo "/tmp/hadoop/bin/hadoop fs -put /tmp/random.10G.touch /alluxio_storage/random.10G.touch"

    echo "bin/alluxio fs ls -f /"
    echo "bin/alluxio fs distributedLoad /random.10G.touch"

    echo "time bin/alluxio fs cat /random.10G.touch | wc -c"
    echo "jps | grep AlluxioWorker ; WORKER_PROC=$(jps | grep AlluxioWorker | awk '{ print $1}')"
    echo "time bin/alluxio fs cat /random.10G.touch | wc -c  & sleep 2; kill -9 \$WORKER_PROC ; fg %1  # read again"

    echo "tail -f logs/user/user_ec2-user.log"
    
    echo "alluxio fsadmin report"
    echo "sleep 600"
    echo "time bin/alluxio fs cat /random.10G.touch | wc -c"
}