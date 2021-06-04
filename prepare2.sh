# load all tpcds data from S3 to HDFS
hdfs dfs -mkdir /tmp/tpcds/
s3-dist-cp --src s3://alluxio.saiguang.test/tpcds/parquet/scale100/ --dest /tmp/tpcds/

# create all tpcds tables
aws s3 cp s3://alluxio.saiguang.test/tpcds/ddl.tar.gz ddl.tar.gz
tar -xvf ddl.tar.gz
sed "s/nameservice1:8092/\/tmp\/tpcds/" template/* > create-table.sql
hive -f create-table.sql

echo "
MSCK REPAIR TABLE catalog_returns;
MSCK REPAIR TABLE catalog_sales;
MSCK REPAIR TABLE inventory;
MSCK REPAIR TABLE store_returns;
MSCK REPAIR TABLE store_sales;
MSCK REPAIR TABLE web_returns;
MSCK REPAIR TABLE web_sales;
" >> create-table.sql

# load all tpcds queries
mkdir -p queries
aws s3 cp --recursive s3://alluxio.saiguang.test/tpcds/queries-tpcds_2_4_presto/ queries

# update core-site.xml
core_site_path=/etc/presto/conf/alluxioConf/core-site.xml
sudo cp $core_site_path $core_site_path.alluxio # back up the alluxio config

targetLine=$(cat $core_site_path | grep "alluxio.hadoop.ShimFileSystem" -n  | cut -d: -f1)
sed "$((targetLine-2)),$((targetLine+5))d" /etc/presto/conf/alluxioConf/core-site.xml | sudo tee $core_site_path.hdfs # create hdfs config

# example config updates
echo "sudo cp $core_site.hdfs $core_site"
echo "sudo cp $core_site.alluxio $core_site"
# example query execs
echo "presto-cli --catalog onprem --schema default < queries/q44.sql"