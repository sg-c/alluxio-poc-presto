#!/bin/bash

alluxio fs mount /s3 s3://autobots-tpcds-pregenerated-data/spark/unpart_sf100_10k/

cp create-table.template.sql create-table.alluxio.sql
sed -i "s/DATA_LOC_SALES/alluxio:\/\/`hostname -f`:19998\/s3\/store_sales/" create-table.alluxio.sql
sed -i "s/DATA_LOC_ITEM/alluxio:\/\/`hostname -f`:19998\/s3\/item/"         create-table.alluxio.sql
sed -i "s/TABLE_TYPE/alluxio/"                                              create-table.alluxio.sql

cp q44.template.sql q44.alluxio.sql
sed -i "s/TABLE_TYPE/alluxio/" q44.alluxio.sql

hive -f create-table.alluxio.sql


cp create-table.template.sql create-table.s3.sql
sed -i "s/DATA_LOC_SALES/s3:\/\/autobots-tpcds-pregenerated-data\/spark\/unpart_sf100_10k\/store_sales/" create-table.s3.sql
sed -i "s/DATA_LOC_ITEM/s3:\/\/autobots-tpcds-pregenerated-data\/spark\/unpart_sf100_10k\/item/"         create-table.s3.sql
sed -i "s/TABLE_TYPE/s3/"                                                                                create-table.s3.sql

cp q44.template.sql q44.s3.sql
sed -i "s/TABLE_TYPE/s3/" q44.s3.sql

hive -f create-table.s3.sql

# Query on alluxio:     presto-cli --catalog onprem --schema default < q44.alluxio.sql
# Query on s3:          presto-cli --catalog onprem --schema default < q44.s3.sql
# Check alluxio cache:  alluxio fsadmin report