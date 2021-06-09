function download_tpcds_queries() {
    mkdir -p queries
    aws s3 cp --recursive s3://alluxio.saiguang.test/tpcds/queries-tpcds_2_4_presto/ queries
}

function enable_transparent_uri() {
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

function disable_transparent_uri() {
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

function show_mount_s3() {
    echo "alluxio fs mount /s3-tpcds s3://autobots-tpcds-pregenerated-data/spark/unpart_sf100_10k/"
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

function show_add_policy() {
    echo "
    alluxio fs policy add \\
        /union_hdfs/tmp/tpcds \\
        \"tpcds_copy:ufsMigrate(olderThan(2s), UFS[hdfs_comp]:STORE)\"
    "
}

function prepare_usage() {
    echo "download_tpcds_queries"
    echo "enable_transparent_uri"
    echo "disable_transparent_uri"
    echo "show_mount_s3"
    echo "show_mount_union"
    echo "run_presto_query"
}
