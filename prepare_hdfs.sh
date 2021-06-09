function prepare_tpcds_data() {

    hadoop fs -test -d /tmp/tpcds

    if [ $? -ne 0 ]; then
        echo "Download TPC-DS data to HDFS..."
        hdfs dfs -mkdir /tmp/tpcds/
        s3-dist-cp --src s3://alluxio.saiguang.test/tpcds/parquet/scale100/ --dest /tmp/tpcds/
    fi

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

function prepare_usage() {
    echo "prepare_tpcds_data"
}