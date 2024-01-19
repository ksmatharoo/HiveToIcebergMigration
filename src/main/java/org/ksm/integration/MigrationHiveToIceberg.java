package org.ksm.integration;

import com.google.common.collect.Maps;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.spark.actions.MigrateTable;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.stream.Collectors;


@Log4j2
public class MigrationHiveToIceberg {


    public static void createAndLoadHiveTable(SparkSession spark,Dataset<Row> dataset,
                                              String tableName,
                                              String dbName,
                                              String basePath,
                                              String partitionKeys) throws Exception{



        HiveClient hiveClient = new HiveClient("src/main/resources/hive-site.xml");
        List<String> keys = Arrays.asList(partitionKeys.split(","));

        hiveClient.prepareTable(dbName, tableName,
                dataset.schema(), keys, basePath);

        dataset.write().partitionBy(keys.toArray(new String[0])).mode(SaveMode.Overwrite).
                parquet(basePath);

        List<Column> partitionColNames = keys.stream().map(name -> {
            return dataset.col(name);
        }).collect(Collectors.toList());

        Table table = hiveClient.getMetaStoreClient().getTable(dbName, tableName);
        List<Row> list = dataset.select(partitionColNames.toArray(new Column[0])).distinct().collectAsList();

        for (Row row : list) {
            List<String> partitionKeyValues = new ArrayList<>();
            List<String> partitionPathList = new ArrayList<>();

            for(String key : keys) {
                String partitionKeyValue = (String)row.getAs(key);
                partitionKeyValues.add(partitionKeyValue);
                partitionPathList.add(String.format("/%s=%s",key,partitionKeyValue));
            }

            String partitionPath = StringUtils.join(partitionPathList,'/');
            hiveClient.addPartition(table, partitionKeyValues, partitionPath);

            //spark.sql("select * from " + dbName + "." + tableName).show(100, false);
        }
        spark.sql("show partitions " + dbName + "." + tableName).show(100, false);
        spark.sql("select count(*) from " + dbName + "." + tableName).show(100, false);
    }


    public static void main(String[] args) throws Exception {
        String warehousePath = "/home/ksm/github/HiveToIcebergMigration/data/";
        String thriftServer = "thrift://172.18.0.5:9083";

        SparkSession spark = Utils.getSparkSession(warehousePath,thriftServer);
        //spark.sparkContext().setLogLevel("ALL");

        String tableName = "testMigrationTable";
        String dbname = "testKsm";
        String partitionKeys = "hire_date,department_id";
        String basePath = warehousePath + tableName;

        spark.sql("create database if not exists "+dbname).show( false);

        Dataset<Row> ds = SparkUtils.readCSVFileWithoutDate(spark, null);
        createAndLoadHiveTable(spark,ds,tableName,dbname,basePath,partitionKeys);

        String fullTableName = dbname + "." + tableName;
        String backupTableSuffix = "backup_tab_";
        MigrateTable migrateTable = new MigrateTable(spark ,fullTableName ,
                backupTableSuffix, Maps.newHashMap());

        long dataFileCount = migrateTable.executeMigration();

        log.info("{} files added to iceberg table {}",dataFileCount,fullTableName);

    }
}
