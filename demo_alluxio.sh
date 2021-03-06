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

    echo "alluxio fs mount /hdfs_comp hdfs://${1}:8020/"
}

function show_commands_hdfs() {
    echo "run \"prepare_tpcds_data_hdfs\" on \"on_prem\" to prepare data"

    echo "alluxio fs ls         /hdfs_comp/tmp"
    echo "alluxio fs checksum   /hdfs_comp/tmp/foo"
    echo "alluxio fs stat       /hdfs_comp/tmp/foo"
    echo "alluxio fs mkdir      /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs ls         /hdfs_comp/tmp"
    echo "alluxio fs chmod 666  /hdfs_comp/tmp/foo"
    echo "alluxio fs ls         /hdfs_comp/tmp"
    echo "hadoop  fs -ls        /hdfs_comp/tmp"
    echo "alluxio fs chown alluxio:alluxio /hdfs_comp/tmp/foo"
    echo "alluxio fs ls         /hdfs_comp/tmp" 
    echo "hadoop  fs -ls        /hdfs_comp/tmp"
    echo "alluxio fs du -h      /hdfs_comp/tmp"
    echo "alluxio fs copyFromLocal /tmp/foo /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs ls         /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs rm -R      /hdfs_comp/tmp/test-mkdir"
    echo "alluxio fs ls         /hdfs_comp/tmp" 
    echo "hadoop  fs -ls        /hdfs_comp/tmp"
}

function show_metadata_sync_hdfs() {
    echo "[active sync]"
    echo "alluxio fs startSync /hdfs_comp"
    echo "alluxio fs getSyncPathList"
    echo "hadoop fs -mkdir /tmp/metadata-sync-test"
    echo "alluxio fs ls /hdfs_comp/tmp/metadata-sync-test"
    echo "hadoop fs -copyFromLocal /tmp/foo /tmp/metadata-sync-test/foo1"
    echo "alluxio fs ls /hdfs_comp/tmp/metadata-sync-test"

    echo "[manual sync]"
    echo "alluxio fs stopSync /hdfs_comp"
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
    echo "alluxio fs startSync /hdfs_comp"
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
    echo "alluxio fs mkdir /hdfs_comp/tmp/write-test"
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
    echo "alluxio fs ls         /s3/tmp"
    echo "alluxio fs checksum   /s3/tmp/foo"
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

    echo "[reset]"
    echo "alluxio fs rm -R /s3/tmp/metadata-sync-test"
}

function show_read_s3() {
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/read-test/foo.cache"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/tmp/read-test/foo.no_cache"

    echo "alluxio fs ls /s3/tmp/read-test"
    echo "alluxio fs cat /s3/tmp/read-test/foo.cache"
    echo "alluxio fs -Dalluxio.user.file.readtype.default=NO_CACHE cat /s3/tmp/read-test/foo.no_cache"
    echo "alluxio fs ls /s3/tmp/read-test"

    echo "[reset]"
    echo "alluxio fs rm -R /s3/tmp/read-test"
}

function show_write_s3() {
    echo "alluxio fs mkdir /s3/tmp/write-test"

    echo "alluxio fs -Dalluxio.user.file.writetype.default=MUST_CACHE copyFromLocal /tmp/foo /s3/tmp/write-test/foo.must_cache"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH copyFromLocal /tmp/foo /s3/tmp/write-test/foo.cache_through"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=ASYNC_THROUGH copyFromLocal /tmp/foo /s3/tmp/write-test/foo.async_through"
    echo "alluxio fs -Dalluxio.user.file.writetype.default=THROUGH copyFromLocal /tmp/foo /s3/tmp/write-test/foo.through"

    echo "alluxio fs ls /s3/tmp/write-test"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/tmp/write-test/"
}

function show_stat_s3() {
    echo "alluxio fs stat /s3/tmp/write-test/foo.must_cache"

    echo "[reset]"
    echo "alluxio fs rm -R /s3/tmp/write-test"
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
    
    doas alluxio "alluxio-mount.sh SudoMount local"  # sudo mount ramdisk
    doas alluxio "alluxio-start.sh worker"
}

function show_use_fuse() {
    echo "ls /mnt/alluxio-fuse"
    echo "ls /mnt/alluxio-fuse/s3"
    echo "ls /mnt/alluxio-fuse/hdfs_comp"

    echo "cp /tmp/foo /mnt/alluxio-fuse/s3/tmp/foo.posix"
    echo "cp /tmp/foo /mnt/alluxio-fuse/hdfs_comp/tmp/foo.posix"

    echo "ls /mnt/alluxio-fuse/s3/tmp/"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/tmp/"
    echo "ls /mnt/alluxio-fuse/hdfs_comp/tmp/"
    echo "hadoop fs -ls /tmp"

    echo "cat /mnt/alluxio-fuse/s3/tmp/foo.posix"
    echo "aws s3 cp s3://alluxio.saiguang.test/demo/tmp/foo.posix -"
    echo "cat /mnt/alluxio-fuse/hdfs_comp/tmp/foo.posix"
    echo "hadoop fs -cat /tmp/foo.posix"

    echo "rm /mnt/alluxio-fuse/s3/tmp/foo.posix"
    echo "rm /mnt/alluxio-fuse/hdfs_comp/tmp/foo.posix"
}

function show_unified_namespace() {
    echo "alluxio fs ls /hdfs_comp/tmp"
    echo "alluxio fs ls /s3/tmp"
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
    echo "[enable]"

    echo "sudo cp /etc/hadoop/conf/core-site.xml.transparent_uri /etc/hadoop/conf/core-site.xml"
    echo "sudo initctl stop hadoop-yarn-nodemanager"
    echo "sudo initctl start hadoop-yarn-nodemanager"

    echo "alluxio fs mount /s3-tpcds s3://alluxio.saiguang.test/tpcds/parquet/scale100/"

    echo "spark-shell"
    echo "spark.read.parquet(\"s3://alluxio.saiguang.test/tpcds/parquet/scale100/customer\").count"
    echo "spark.read.parquet(\"hdfs://ON_PREM_HOST:8020/tmp/tpcds/customer\").count"

    echo "[disable]"

    echo "sudo cp /etc/hadoop/conf/core-site.xml.orig /etc/hadoop/conf/core-site.xml"
    echo "sudo initctl stop hadoop-yarn-nodemanager"
    echo "sudo initctl start hadoop-yarn-nodemanager"
}

function show_pddm_hdfs() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: show_mount_union COMPUTE_DNS ON_PREM_DNS"
        return 1
    fi

    echo "### mount union ufs ###"
    echo "
    alluxio fs mount \\
        --option alluxio-union.hdfs_store.uri=\"hdfs://${2}:8020/\" \\
        --option alluxio-union.hdfs_store.option.alluxio.underfs.hdfs.configuration=/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml \\
        --option alluxio-union.hdfs_comp.uri=\"hdfs://${1}:8020/\" \\
        --option alluxio-union.hdfs_comp.option.alluxio.underfs.hdfs.configuration=/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml \\
        --option alluxio-union.priority.read=hdfs_comp,hdfs_store \\
        --option alluxio-union.collection.create=hdfs_comp  \\
        /union_hdfs union://union_hdfs/
    "
    echo "alluxio fs mount"
    
    echo "### show union ufs access ###"
    echo "echo hello on prem hdfs | hadoop fs -put - hdfs://${2}:8020/tmp/foo.on_prem"
    echo "echo hello compute hdfs | hadoop fs -put - hdfs://${1}:8020/tmp/foo.compute"
    echo "hadoop fs -ls hdfs://${1}:8020/tmp/"
    echo "hadoop fs -ls hdfs://${2}:8020/tmp/"
    echo "hadoop fs -cat hdfs://${1}:8020/tmp/foo.compute"
    echo "hadoop fs -cat hdfs://${2}:8020/tmp/foo.on_prem"
    echo "alluxio fs cat /union_hdfs/tmp/foo.compute"
    echo "alluxio fs cat /union_hdfs/tmp/foo.on_prem"

    echo "### policy for copy ###"
    echo "alluxio fs load /union_hdfs/tmp/tpcds/customer"  # TODO: ask about loading
    echo "alluxio getConf alluxio.policy.scan.interval"
    echo "hadoop fs -ls hdfs://${1}:8020/tmp/"
    echo "hadoop fs -ls hdfs://${1}:8020/tmp/tpcds/"
    echo "alluxio fs policy add /union_hdfs/tmp/tpcds/customer \\
        \"tpcds_copy:ufsMigrate(olderThan(2s), UFS[hdfs_store]:REMOVE, UFS[hdfs_comp]:STORE)\""
    echo "alluxio fs policy status tpcds_copy"
    echo "hadoop fs -ls hdfs://${1}:8020/tmp/tpcds/"
    echo "hadoop fs -ls hdfs://${2}:8020/tmp/tpcds/"
    echo "alluxio fs cat /union_hdfs/tmp/tpcds/customer/part-00000-2b06809a-b56f-4d3f-a2fc-5cfa09bc7651-c000.snappy.parquet | less"

    echo "### move back data ###"
    echo "hadoop fs -mkdir hdfs://${2}:8020/tmp/tpcds/customer"
    echo "hadoop fs -cp hdfs://${1}:8020/tmp/tpcds/customer/_SUCCESS hdfs://${2}:8020/tmp/tpcds/customer/_SUCCESS"
    echo "hadoop fs -cp hdfs://${1}:8020/tmp/tpcds/customer/part-00000-2b06809a-b56f-4d3f-a2fc-5cfa09bc7651-c000.snappy.parquet\\
        hdfs://${2}:8020/tmp/tpcds/customer/part-00000-2b06809a-b56f-4d3f-a2fc-5cfa09bc7651-c000.snappy.parquet"
    echo "hadoop fs -ls hdfs://${1}:8020/tmp/tpcds/"
    echo "hadoop fs -ls hdfs://${2}:8020/tmp/tpcds/"
}

function show_pddm_nfs_s3() {
    echo "### mount union ufs ###"
    echo "
    alluxio fs mount \\
        --option alluxio-union.union_s3.uri=\"s3://alluxio.saiguang.test/demo/\" \\
        --option alluxio-union.union_nfs.uri=\"/mnt/nfs\" \\
        --option alluxio-union.priority.read=union_nfs,union_s3 \\
        --option alluxio-union.collection.create=union_nfs,union_s3  \\
        /union_nfs_s3 union://nfs_s3/
    "
    echo "alluxio fs mount"
    
    echo "### show union ufs access ###"
    echo "echo hi, s3 | aws s3 cp - s3://alluxio.saiguang.test/demo/foo.union_s3"
    echo "echo hi, s3 | aws s3 cp - s3://alluxio.saiguang.test/demo/foo.union_common"
    echo "aws s3 cp s3://alluxio.saiguang.test/demo/foo.union_s3 -"
    echo "echo hi, nfs > /mnt/nfs/foo.union_nfs"
    echo "echo hi, nfs > /mnt/nfs/foo.union_common"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/"
    echo "ls -ls /mnt/nfs"
    echo "alluxio fs ls -f /union_nfs_s3 | grep foo"

    echo "### show write to union ufs ###"
    echo "ls -l /mnt/nfs/foo.write.nfs_only"
    echo "alluxio fs copyFromLocal /tmp/foo /union_nfs_s3/foo.write.nfs_only"
    echo "ls -l /mnt/nfs/foo.write.nfs_only"
    echo "cat /mnt/nfs/foo.write.nfs_only"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/foo/foo.write.nfs_only"
    echo "aws s3 cp s3://alluxio.saiguang.test/demo/foo/foo.write.nfs_only -"
    echo "# remount union_nfs_s3"
    echo "alluxio fs copyFromLocal /tmp/foo /union_nfs_s3/foo.write.all"
    echo "cat /mnt/nfs/foo.write.all"
    echo "aws s3 cp s3://alluxio.saiguang.test/demo/foo.write.all -"

    echo "### policy for copy ###"
    echo "alluxio fs rm -R /union_nfs_s3/dir-sync"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/dir-sync/foo1"
    echo "aws s3 cp /tmp/foo s3://alluxio.saiguang.test/demo/dir-sync/foo2"
    echo "ls -l /mnt/nfs/dir-sync"
    echo "alluxio fs ls -f /union_nfs_s3/dir-sync"
    echo "alluxio fs policy add /union_nfs_s3/dir-sync \\
        \"copy_data:ufsMigrate(olderThan(2s), UFS[union_nfs]:STORE)\""
    echo "alluxio fs policy status copy_data"
    echo "ls -l /mnt/nfs/dir-sync"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/dir-sync/"

    echo "alluxio fs mkdir /nfs/dir-sync"
    echo "alluxio fs copyFromLocal /tmp/foo /nfs/dir-sync/foo1"
    echo "alluxio fs copyFromLocal /tmp/foo /nfs/dir-sync/foo2"
    echo "alluxio getConf alluxio.policy.scan.interval"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/dir-sync/"
    echo "ls -l /mnt/nfs/dir-sync/"
    echo "alluxio fs ls -f /union_nfs_s3/dir-sync"
    echo "alluxio fs policy add /union_nfs_s3/dir-sync \\
        \"move_data:ufsMigrate(olderThan(2s), UFS[union_nfs]:REMOVE, UFS[union_s3]:STORE)\""
    echo "alluxio fs policy status move_data"
    echo "aws s3 ls s3://alluxio.saiguang.test/demo/dir-sync/"
    echo "ls -l /mnt/nfs/dir-sync/"

    echo "### move back data ###"
    echo "hadoop fs -mkdir hdfs://${2}:8020/tmp/tpcds/customer"
    echo "hadoop fs -cp hdfs://${1}:8020/tmp/tpcds/customer/_SUCCESS hdfs://${2}:8020/tmp/tpcds/customer/_SUCCESS"
    echo "hadoop fs -cp hdfs://${1}:8020/tmp/tpcds/customer/part-00000-2b06809a-b56f-4d3f-a2fc-5cfa09bc7651-c000.snappy.parquet\\
        hdfs://${2}:8020/tmp/tpcds/customer/part-00000-2b06809a-b56f-4d3f-a2fc-5cfa09bc7651-c000.snappy.parquet"
    echo "hadoop fs -ls hdfs://${1}:8020/tmp/tpcds/"
    echo "hadoop fs -ls hdfs://${2}:8020/tmp/tpcds/"
}

function show_sds() {
    echo "### prepare ###"
    echo "hive -e \\
        \"CREATE TABLE IF NOT EXISTS default.geo (\\
            truckid  string,\\
            driverid string,\\
            event    string,\\
            latitude    double,\\
            longtitude  double,\\
            city    string,\\
            state   string,\\
            velocity    int,\\
            event_idx   int,\\
            idling_idx  int)\\
        ROW FORMAT DELIMITED\\
        FIELDS TERMINATED BY ','\\
        LINES TERMINATED BY '\n'\\
        LOCATION 'hdfs:///geolocation';\""
    echo "s3-dist-cp --src s3://alluxio.saiguang.test/geo-data/ --dest /geolocation"

    echo "### attach db ###"
    echo "alluxio table ls"
    echo "alluxio table attachdb --db hive_compute hive \\
    thrift://$(alluxio getConf alluxio.master.hostname):9083 default  # attach db to alluxio"
    echo "alluxio table ls"
    echo "alluxio table ls hive_compute"
    echo "hive -e \"show tables;\""

    echo "### query ###"
    echo "presto-cli --execute \"show schemas in catalog_alluxio\"  # show schemas"
    echo "presto-cli --execute \"show tables in catalog_alluxio.hive_compute\"  # show tables"
    echo "presto-cli --execute \"select * from catalog_alluxio.hive_compute.geo limit 5\" # run query"
    echo "hive -e \"select * from geo limit 5\""

    echo "### transform ###"
    echo "hadoop fs -ls /geolocation"
    echo "alluxio fs ls /catalog/hive_compute/tables/geo/  # only hive dir"
    echo "alluxio table transform hive_compute geo  # start transform"
    echo "alluxio table transformStatus JOB_ID  # show transform status"
    echo "alluxio fs ls /catalog/hive_compute/tables/geo/  # new _internal_ dir"
    echo "hadoop fs -rm -r /geolocation  # new _internal_ dir"
    echo "hive -e \"select * from geo limit 5\""
    echo "presto-cli --execute \"select * from catalog_alluxio.hive_compute.geo limit 5\" # run query"

    echo "### detach db ###"
    echo "alluxio table detachdb hive_compute  # detach db from alluxio"
    echo "alluxio table ls"
}

function  show_orchestration_hub() {
    if [ "$#" -ne 1 ]; then
        echo "Usage: show_orchestration_hub COMPUTE_DNS"
        return 1
    fi

    echo "open http://${1}:30077"
}

# TODO: Cannot restore UFS
function show_backup() {
    if [ "$#" -ne 1 ]; then
        echo "Usage: show_backup ON_PREM_DNS"
        return 1
    fi

    echo "### create backup ###"
    echo "sudo su - alluxio"
    echo "hadoop fs -ls hdfs://${1}:8020/alluxio_backups"
    echo "alluxio fsadmin backup"
    echo "hadoop fs -ls hdfs://${1}:8020/alluxio_backups"

    echo "### format journal ###"
    echo "alluxio fs mount"
    echo "alluxio-stop.sh master"
    echo "alluxio format"
    echo "alluxio-start.sh master"
    echo "alluxio fs mount"

    echo "### restore backup ###"
    echo "alluxio-stop.sh master"
    echo "alluxio format"
    echo "alluxio-start.sh -i hdfs://${1}:8020/alluxio_backups/BACKUP.gz master"
    echo "alluxio fs mount"
}

function show_multiple_medium() {
    echo "### allocator values ###"
    echo "set alluxio.worker.allocator.class to following values"
    echo "alluxio.worker.block.allocator.GreedyAllocator"
    echo "alluxio.worker.block.allocator.MaxFreeAllocator"
    echo "alluxio.worker.block.allocator.RoundRobinAllocator"

    echo "### restart worker ###"
    echo "sudo su - alluxio"
    echo "alluxio-start.sh worker"
    echo "alluxio fs free /"
    echo "alluxio fs copyFromLocal /tmp/foo /tmp/medium-test/foo.$(date +%s)"

    echo "### check worker path ###"
    echo "ls /mnt/ramdisk/alluxioworker"
    echo "ls /mnt/alluxio/alluxioworker"
}

function show_ha_master() {
    echo "[on Alluxio SandBox master /tmp/alluxio]"

    echo "bin/alluxio fs masterInfo"
    echo "bin/alluxio runTest"

    echo "jps | grep AlluxioMaster ; MASTER_PROC=$(jps | grep AlluxioMaster | awk '{ print $1}')"
    echo "bin/alluxio runTest & sleep 3; kill -9 \$MASTER_PROC ; fg %1"
    echo "bin/alluxio fs masterInfo"

    echo "bin/alluxio-start.sh master"
    echo "bin/alluxio fs masterInfo"
}

function show_ha_worker() {
    echo "[on Alluxio SandBox worker /tmp/alluxio]"

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

function show_job_servce() {
    echo "### distributedLoad ###"
    echo "alluxio fs free /"
    echo "alluxio fs distributedLoad /tmp/tpcds/customer"
    echo "alluxio fs ls /tmp/tpcds/customer"

    echo "### distributedCopy ###"
    echo "hadoop fs -ls /tmp/customer"
    echo "alluxio fs distributedCp /tmp/tpcds/customer /hdfs_comp/tmp/customer"
    echo "alluxio fs ls /hdfs_comp/tmp"
    echo "alluxio fs ls /hdfs_comp/tmp/customer"
    echo "hadoop fs -ls /tmp/customer"

    echo "### distributedMove ###"
    echo "hadoop fs -ls /tmp/customer-new"
    echo "alluxio fs distributedMv /hdfs_comp/tmp/customer /hdfs_comp/tmp/customer-new"
    echo "alluxio fs ls /hdfs_comp/tmp"
    echo "hadoop fs -ls /tmp/customer-new"

    echo "### job monitoring ###"
    echo "alluxio job ls"
    echo "alluxio job stat JOB_ID"
}

function show_collect_info() {
    echo "### prepare ssh to localhost ###"
    echo "sudo su - hadoop"
    echo "ssh-keygen"
    echo "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"
    echo "### collect info for local ###"
    echo "alluxio collectInfo --local all /tmp/alluxio-diagnose"
    echo "ls /tmp/alluxio-diagnose"
    echo "less /tmp/alluxio-diagnose/collectEnv/collectEnv.txt"
}

# TODO: revisit
function show_audit() {
    echo "less /opt/alluxio/logs/master_audit.log"
}

function show_acl() {
    echo "### create user bob ###"
    echo "sudo useradd bob"

    echo "### access only by hadoop ###"
    echo "sudo su - hadoop"
    echo "alluxio fs copyFromLocal /tmp/foo /tmp/foo.hadoop"
    echo "alluxio fs chmod 600 /tmp/foo.hadoop"
    echo "alluxio fs getfacl /tmp/foo.hadoop"
    echo "sudo su - bob"
    echo "alluxio fs cat /tmp/foo.hadoop"

    echo "### update acl for bob ###"
    echo "sudo su - hadoop"
    echo "alluxio fs setfacl -m \"user:bob:r-x\" /tmp/foo.hadoop"
    echo "alluxio fs getfacl /tmp/foo.hadoop"

    echo "### accessible by bob ###"
    echo "sudo su - bob"
    echo "alluxio fs cat /tmp/foo.hadoop"
}