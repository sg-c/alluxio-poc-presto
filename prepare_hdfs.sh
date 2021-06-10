


function prepare_tpcds_data_hdfs() {

    hadoop fs -test -d /tmp/tpcds

    if [ $? -ne 0 ]; then
        echo "Download TPC-DS data to HDFS..."
        hdfs dfs -mkdir /tmp/tpcds/
        s3-dist-cp --src s3://alluxio.saiguang.test/tpcds/parquet/scale100/ --dest /tmp/tpcds/
    fi

    echo "Create Hive Tables..."

    echo "
        CREATE DATABASE IF NOT EXISTS tpcds_hdfs;
        USE default;
    " > create-table-hdfs.sql

    aws s3 cp s3://alluxio.saiguang.test/tpcds/ddl.tar.gz ddl.tar.gz
    tar -xvf ddl.tar.gz
    sed "s/nameservice1:8092/\/tmp\/tpcds/" template/* >> create-table-hdfs.sql

    echo "
    MSCK REPAIR TABLE catalog_returns;
    MSCK REPAIR TABLE catalog_sales;
    MSCK REPAIR TABLE inventory;
    MSCK REPAIR TABLE store_returns;
    MSCK REPAIR TABLE store_sales;
    MSCK REPAIR TABLE web_returns;
    MSCK REPAIR TABLE web_sales;
    " >> create-table-hdfs.sql

    hive -f create-table-hdfs.sql
}


function prepare_tpcds_data_s3() {
    echo "Create Hive Tables..."

    echo "
        CREATE DATABASE IF NOT EXISTS tpcds_s3;
        USE tpcds_s3;
    " > create-table-s3.sql

    aws s3 cp s3://alluxio.saiguang.test/tpcds/ddl.tar.gz ddl.tar.gz
    tar -xvf ddl.tar.gz
    sed "s/hdfs:\/\/nameservice1:8092/s3:\/\/alluxio.saiguang.test\/tpcds\/parquet\/scale100/" template/* >> create-table-s3.sql

    echo "
    MSCK REPAIR TABLE catalog_returns;
    MSCK REPAIR TABLE catalog_sales;
    MSCK REPAIR TABLE inventory;
    MSCK REPAIR TABLE store_returns;
    MSCK REPAIR TABLE store_sales;
    MSCK REPAIR TABLE web_returns;
    MSCK REPAIR TABLE web_sales;
    " >> create-table-s3.sql

    hive -f create-table-s3.sql
}

function prepare_usage() {
    echo "prepare_tpcds_data"
}