function prepare_tpcds_data() {
    echo "Download TPC-DS data to HDFS..."
    hdfs dfs -mkdir /tmp/tpcds/
    s3-dist-cp --src s3://alluxio.saiguang.test/tpcds/parquet/scale100/ --dest /tmp/tpcds/

    echo "Create Hive Tables..."
    aws s3 cp s3://alluxio.saiguang.test/tpcds/ddl.tar.gz ddl.tar.gz
    tar -xvf ddl.tar.gz
    sed "s/nameservice1:8092/\/tmp\/tpcds/" template/* > create-table.sql

    echo "
    MSCK REPAIR TABLE catalog_returns;
    MSCK REPAIR TABLE catalog_sales;
    MSCK REPAIR TABLE inventory;
    MSCK REPAIR TABLE store_returns;
    MSCK REPAIR TABLE store_sales;
    MSCK REPAIR TABLE web_returns;
    MSCK REPAIR TABLE web_sales;
    " >> create-table.sql

    hive -f create-table.sql
}

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


# # update core-site.xml
# core_site_path=/etc/presto/conf/alluxioConf/core-site.xml
# sudo cp $core_site_path $core_site_path.alluxio # back up the alluxio config

# targetLine=$(cat $core_site_path | grep "alluxio.hadoop.ShimFileSystem" -n  | cut -d: -f1)
# sed "$((targetLine-2)),$((targetLine+5))d" /etc/presto/conf/alluxioConf/core-site.xml | sudo tee $core_site_path.hdfs > /dev/null # create hdfs config

# # example config updates
# echo "sudo cp $core_site_path.hdfs $core_site_path"
# echo "sudo cp $core_site_path.alluxio $core_site_path"
# # example query execs
# echo "presto-cli --catalog onprem --schema default < queries/q44.sql"
# # restart hdfs services
# echo "sudo initctl stop presto-server; sudo initctl start presto-server"
# # check presto alluxio config
# echo "cat /etc/presto/conf/alluxioConf/core-site.xml | grep 'alluxio.hadoop.ShimFileSystem' -A5 -B2"

alluxio fs mount \
   --option alluxio-union.hdfs_store.uri="hdfs://ip-84-13-11-160.us-west-1.compute.internal:8020/" \
   --option alluxio-union.hdfs_store.option.alluxio.underfs.hdfs.configuration=/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml \
   --option alluxio-union.hdfs_comp.uri="hdfs://ip-124-75-3-215.ec2.internal:8020/" \
   --option alluxio-union.hdfs_comp.option.alluxio.underfs.hdfs.configuration=/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml \
   --option alluxio-union.priority.read=hdfs_comp,hdfs_store \
   --option alluxio-union.collection.create=hdfs_comp  \
   /union_hdfs union://union_hdfs/

echo "hello on prem hdfs" | hadoop fs -put - hdfs://ip-84-13-11-160.us-west-1.compute.internal:8020/tmp/foo.on_prem
echo "hi compute hdfs" | hadoop fs -put - hdfs://ip-124-75-3-215.ec2.internal:8020/tmp/foo.compute

alluxio fs policy add \
  /union_hdfs/tmp/tpcds \
  "tpcds_copy:ufsMigrate(olderThan(2s), UFS[hdfs_comp]:STORE)"

alluxio fs policy status tpcds_copy
# trigger PDDM data copy 
echo "alluxio fs ls /union_hdfs/tmp"

# mount s3 to alluxio
alluxio fs mount /s3-tpcds s3://autobots-tpcds-pregenerated-data/spark/unpart_sf100_10k/