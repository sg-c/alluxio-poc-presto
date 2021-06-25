function show_worker_reliability() {

    # ssh -i "~/.ssh/wa5oj.pem" centos@ec2-34-229-98-7.compute-1.amazonaws.com
    # ssh -i "~/.ssh/wa5oj.pem" centos@ec2-54-81-157-114.compute-1.amazonaws.com
    # ssh -i "~/.ssh/wa5oj.pem" centos@ec2-3-92-78-167.compute-1.amazonaws.com  WORKER-1
    # ssh -i "~/.ssh/wa5oj.pem" centos@ec2-3-84-73-217.compute-1.amazonaws.com
    
    echo "

    head -c 10G </dev/urandom > /tmp/random.10G.touch   # generate a random file of 10G 
    hadoop fs -put /tmp/random.10G.touch /  # put the file to HDFS, UFS of Alluxio
    bin/alluxio fs ls -f /  # force load the metadata of random.10G.touch
    bin/alluxio fs distributedLoad /random.10G.touch # load data of random.10G.touch to alluxio
    time bin/alluxio fs cat /random.10G.touch | wc -c  # read the file and show the time
    jps | grep AlluxioWorker ; WORKER_PROC=$(jps | grep AlluxioWorker | awk '{ print $1}')  # find and record the current worker process id
    time bin/alluxio fs cat /random.10G.touch | wc -c  & sleep 2; kill -9 $WORKER_PROC ; fg %1  # read again
    "
}

function doas() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function doas, expecting 2"
    exit 2
  fi
  local user="$1"
  local cmd="$2"

  sudo runuser -l "${user}" -c "${cmd}"
}

function set_alluxio_property() {
  ALLUXIO_SITE_PROPERTIES=/opt/alluxio/conf/alluxio-site.properties

  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function set_alluxio_property, expecting 2"
    exit 2
  fi
  local property="$1"
  local value="$2"

  if grep -qe "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} 2> /dev/null; then
    doas alluxio "sed -i 's;${property}=.*;${property}=${value};g' ${ALLUXIO_SITE_PROPERTIES}"
    # echo "Property ${property} already exists in ${ALLUXIO_SITE_PROPERTIES} and is replaced with value ${value}" >&2
  else
    doas alluxio "echo '${property}=${value}' >> ${ALLUXIO_SITE_PROPERTIES}"
  fi
}

function download_tpcds_queries() {
    mkdir -p queries
    aws s3 cp --recursive s3://alluxio.saiguang.test/tpcds/queries-tpcds_2_4_presto/ queries
}

function enable_transparent_uri_for_presto() {
    local core_site=/etc/presto/conf/alluxioConf/core-site.xml
    local core_site_allx=$core_site.alluxio

    if [ ! -f "$core_site_allx" ]; then
        sudo cp $core_site $core_site_allx
    fi

    if [ -f "$core_site_allx" ]; then
        echo "Update presto core-site.xml..."
        sudo cp $core_site_allx $core_site

        echo "Restart presto server..."
        sudo initctl stop presto-server; sudo initctl start presto-server
    fi
}

function disable_transparent_uri_for_presto() {
    local core_site=/etc/presto/conf/alluxioConf/core-site.xml
    local core_site_allx=$core_site.alluxio
    local core_site_hdfs=$core_site.hdfs

    if [ ! -f "$core_site_allx" ]; then
        sudo cp $core_site $core_site_allx
    fi

    if [ ! -f "$core_site_hdfs" ]; then
        echo "Update presto core-site.xml..."
        local targetLine=$(cat $core_site | grep "alluxio.hadoop.ShimFileSystem" -n  | cut -d: -f1)
        sed "$((targetLine-2)),$((targetLine+5))d" $core_site_allx | sudo tee $core_site_hdfs > /dev/null # create hdfs config
        sudo cp $core_site_hdfs $core_site

        echo "Restart presto server..."
        sudo initctl stop presto-server; sudo initctl start presto-server
    fi
}

function show_enable_transparent_uri_for_spark() {
    echo "
    sudo vim /etc/hadoop/conf/core-site.xml 
    
    <property>
        <name>fs.hdfs.impl</name>
        <value>alluxio.hadoop.ShimFileSystem</value>
    </property>
    <property>
        <name>fs.AbstractFileSystem.hdfs.impl</name>
        <value>alluxio.hadoop.AlluxioShimFileSystem</value>
    </property>

    --------

    # restart node manager on worker nodes
    
    sudo initctl stop hadoop-yarn-nodemanager
    sudo initctl start hadoop-yarn-nodemanager
    "
}

function enable_transparent_uri_for_s3() {
    local core_site=/etc/hadoop/conf/core-site.xml

    sed -i "s/org.apache.hadoop.fs.s3.EMRFSDelegate/alluxio.hadoop.AlluxioShimFileSystem/" $core_site
    sed -i "s/com.amazon.ws.emr.hadoop.fs.EmrFileSystem/alluxio.hadoop.ShimFileSystem/" $core_site
}

function disable_transparent_uri_for_s3() {
    local core_site=/etc/hadoop/conf/core-site.xml

    sed -i "s/alluxio.hadoop.AlluxioShimFileSystem/org.apache.hadoop.fs.s3.EMRFSDelegate/" $core_site
    sed -i "s/alluxio.hadoop.ShimFileSystem/com.amazon.ws.emr.hadoop.fs.EmrFileSystem/" $core_site
}

function show_mount_s3_tpcds() {
    echo "alluxio fs mount /s3-tpcds s3://alluxio.saiguang.test/tpcds/parquet/scale100/"
}

function show_mount_union() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: show_mount_union COMPUTE_DNS ON_PREM_DNS"
        return 1
    fi

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

    echo "hello on prem hdfs" | hadoop fs -put - hdfs://${2}:8020/tmp/foo.on_prem &
    echo "hello compute hdfs" | hadoop fs -put - hdfs://${1}:8020/tmp/foo.compute &
}

function run_presto_query() {
    if [ $# -ne 1 ]; then
        echo "Usage: run_query queries/qxx.sql"
        return 1
    fi

    presto-cli --catalog onprem --schema default < $1
}

function show_spark_command_alluxio() {
    echo "spark.read.parquet(\"alluxio://$(alluxio getConf alluxio.master.hostname):19998/s3-tpcds/customer/\").count"
}

function show_spark_command_s3() {
    echo "spark.read.parquet(\"s3://alluxio.saiguang.test/tpcds/parquet/scale100/customer/\").count"
}

function show_add_policy() {
    echo "
    alluxio fs policy add \\
        /union_hdfs/tmp/tpcds \\
        \"tpcds_copy:ufsMigrate(olderThan(2s), UFS[hdfs_comp]:STORE)\"
    "
}

function use_max_free_allocator() {
    echo "Update config."
    set_alluxio_property alluxio.worker.allocator.class alluxio.worker.block.allocator.MaxFreeAllocator

    echo "Restart worker"
    doas alluxio "alluxio-start.sh worker"

    echo "Write new files and check new files added to /mnt/alluxio/alluxioworker"
}

function use_greedy_allocator() {
    echo "Update config."
    set_alluxio_property alluxio.worker.allocator.class alluxio.worker.block.allocator.GreedyAllocator

    echo "Restart worker"
    doas alluxio "alluxio-start.sh worker"

    echo "Write new files and check new files added to /mnt/ramdisk/alluxioworker"
}

function use_round_robin_allocator() {
    echo "Update config."
    set_alluxio_property alluxio.worker.allocator.class alluxio.worker.block.allocator.RoundRobinAllocator

    echo "Restart worker"
    doas alluxio "alluxio-start.sh worker"

    echo "Write new files and check new files added to both /mnt/ramdisk/alluxioworker and /mnt/alluxio/alluxioworker"
}

function set_policy_scan_interval() {
    if [ $# -ne 1 ]; then
        echo "Please add the argument for interval value."
        return 1
    fi

    echo "Update config."
    set_alluxio_property alluxio.policy.scan.interval "$1"

    echo "Restart master"
    doas alluxio "alluxio-start.sh master"

    echo "alluxio.policy.scan.interval=$(alluxio getConf alluxio.policy.scan.interval)"
}

function stop_hdfs_namenode() {
    sudo initctl stop hadoop-hdfs-namenode
}

function start_hdfs_namenode() {
    sudo initctl start hadoop-hdfs-namenode
}

function prepare_sds_table() {
    hive -e "
        CREATE TABLE IF NOT EXISTS default.geo (
            truckid  string,
            driverid string,
            event    string,
            latitude    double,
            longtitude  double,
            city    string,
            state   string,
            velocity    int,
            event_idx   int,
            idling_idx  int
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        LOCATION 'hdfs:///geolocation';
    "

    s3-dist-cp --src s3://alluxio.saiguang.test/geo-data/ --dest /geolocation
}

function show_sds_commands() {
    echo "
    alluxio table attachdb --db hive_compute hive thrift://$(alluxio getConf alluxio.master.hostname):9083 default  # attach db to alluxio"

    echo "
    alluxio table detachdb hive_compute  # detach db from alluxio"

    echo "
    alluxio table ls  # list attached db"

    echo "
    alluxio table transform hive_compute geo  # start transform"

    echo "
    alluxio table transformStatus JOB_ID  # show transform status"

    echo "
    presto-cli --execute \"show catalogs\"  # show catalogs"

    echo "
    presto-cli --execute \"show schemas in catalog_alluxio\"  # show schemas"

    echo "
    presto-cli --execute \"show tables in catalog_alluxio.hive_compute\"  # show tables"

    echo "
    presto-cli --execute \"select * from catalog_alluxio.hive_compute.geo limit 20\" # run query"

    echo -e "\n"
}

function enable_auditing() {
    echo "Update config."
    set_alluxio_property alluxio.master.audit.logging.enabled "true"

    echo "Restart master."
    alluxio-start.sh master
}

function disable_auditing() {
    echo "Update config."
    set_alluxio_property alluxio.master.audit.logging.enabled "false"

    echo "Restart master."
    alluxio-start.sh master
}

function set_ttl_checker_interval() {
    echo "Update config."
    set_alluxio_property alluxio.master.ttl.checker.interval "$1"
    
    echo "Restart master."
    doas alluxio "alluxio-start.sh master"

    echo "alluxio.master.ttl.checker.interval=$(alluxio getConf alluxio.master.ttl.checker.interval)"
}

function show_set_ttl() {
    echo "alluxio fs load /tmp/foo; alluxio fs setTtl --action free /tmp/foo 1s;"
}

function show_path_conf_metadata_sync() {
    echo "alluxio fsadmin pathConf add --property alluxio.user.file.metadata.sync.interval=20s ALLU_PATH"
}

function mount_fuse() {
    echo "user_allow_other" | sudo tee /etc/fuse.conf  # config for FUSE

    sudo mkdir -p /mnt/alluxio-fuse
    sudo chmod 777 /mnt/alluxio-fuse

    doas alluxio "/opt/alluxio/integration/fuse/bin/alluxio-fuse mount -o allow_other /mnt/alluxio-fuse /"
}

function umount_fuse() {
    doas alluxio "/opt/alluxio/integration/fuse/bin/alluxio-fuse umount /mnt/alluxio-fuse"
}

function show_mount_hdfs() {
    if [ "$#" -ne 1 ]; then
        echo "Pass hdfs namenode..."
        return 1
    fi

    echo "alluxio fs mount MOUNT_POINT \"hdfs://${1}:8020/\""
}

function show_hdfs_namenodes() {
    hdfs getconf -namenodes
}

function show_journal_backup_test_steps() {
    echo "alluxio-stop.sh master # stop master"
    echo "alluxio format # tear down everything"
    echo "alluxio-start.sh master # start master"
    echo "alluxio fs mount /hdfs_comp hdfs://$(hdfs getconf -namenodes):8020/ # mount 2nd hdfs"
    echo "alluxio fs mount # show all mount points"
    echo "alluxio runTest --directory / # generate files under /"
    echo "alluxio runTest --directory /hdfs_comp # generate files under /hdfs_comp"
    echo "alluxio fs ls -R /default_tests_files # show test files under /"
    echo "alluxio fs ls -R /hdfs_comp/default_tests_files # show test files under /hdfs_comp"
    echo "alluxio fsadmin backup # back up journal"

    echo "write down the backup URI" # hdfs://ip-44-229-5-53.us-west-2.compute.internal:8020/alluxio_backups/alluxio-backup-2021-06-22-1624370826728.gz

    echo "alluxio-stop.sh master # stop master"
    echo "alluxio format # tear down everything"
    echo "alluxio-start.sh master # start master"
    echo "alluxio fs mount # show all mount points"

    echo "alluxio-stop.sh master # stop master"
    echo "alluxio format # tear down everything"
    echo "alluxio-start.sh -i BACKUP_URI master"
    echo "alluxio fs mount # show all mount points"
    echo "alluxio fs ls -R /default_tests_files # show test files under /"
    echo "alluxio fs ls -R /hdfs_comp/default_tests_files # show test files under /hdfs_comp"
}

function prepare_usage() {
    echo -e "\n"

    echo -e "[Cache Acceleration Demo]"
    echo -e "    download_tpcds_queries"
    echo -e "    run_presto_query"
    echo -e "    show_spark_command"
    echo -e "\n"

    echo -e "[PDDM Demo]"
    echo -e "    show_mount_union"
    echo -e "    show_add_policy"
    echo -e "    set_policy_scan_interval"
    echo -e "    start_hdfs_namenode"
    echo -e "    stop_hdfs_namenode"
    echo -e "\n"

    echo -e "[Transparent URI Demo]"
    echo -e "    show_enable_transparent_uri_for_spark"
    echo -e "    enable_transparent_uri_for_s3"
    echo -e "    disable_transparent_uri_for_s3"
    echo -e "    enable_transparent_uri_for_presto"
    echo -e "    disable_transparent_uri_for_presto"
    echo -e "\n"

    echo -e "[Unified Namespace Demo]"
    echo -e "    show_mount_s3"
    echo -e "\n"

    echo -e "[Tiered Storage Demo]"
    echo -e "    use_max_free_allocator"
    echo -e "    use_greedy_allocator"
    echo -e "    use_round_robin_allocator"
    echo -e "\n"

    echo -e "[Catalog Service]"
    echo -e "    prepare_sds_table"
    echo -e "    show_sds_commands"
    echo -e "\n"

    echo -e "[Others]"
    echo -e "    enable_auditing"
    echo -e "    disable_auditing"
    echo -e " "
    echo -e "    set_ttl_checker_interval"
    echo -e "    show_set_ttl"
    echo -e " "
    echo -e "    show_path_conf_metadata_sync"
    echo -e " "
    echo -e "    mount_fuse"
    echo -e "    umount_fuse"
    echo -e " "
    echo -e "    show_mount_hdfs"
    echo -e "    show_hdfs_namenodes"
    echo -e "\n"
}
