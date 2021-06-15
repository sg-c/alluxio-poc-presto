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

function show_mount_s3() {
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

function stop_hdfs_namednoe() {
    sudo initctl stop hadoop-hdfs-namenode
}

function start_hdfs_namednoe() {
    sudo initctl start hadoop-hdfs-namenode
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
    echo -e "    start_hdfs_namednoe"
    echo -e "    stop_hdfs_namenode"
    echo -e "\n"

    echo -e "[Transparent URI Demo]"
    echo -e "    enable_transparent_uri_for_presto"
    echo -e "    disable_transparent_uri_for_presto"
    echo -e "    enable_transparent_uri_for_s3"
    echo -e "    disable_transparent_uri_for_s3"
    echo -e "\n"

    echo -e "[Unified Namespace Demo]"
    echo -e "    show_mount_s3"
    echo -e "\n"

    echo -e "[Tiered Storage Demo]"
    echo -e "    use_max_free_allocator"
    echo -e "    use_max_free_allocator"
    echo -e "\n"
}
