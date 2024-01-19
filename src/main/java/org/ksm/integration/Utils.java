package org.ksm.integration;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class Utils {

    public static SparkSession getSparkSession(String wareHousePath, String thriftServer) throws NoSuchTableException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IcebergTest")
                .master("local[1]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                //.config("spark.sql.warehouse.dir", wareHousePath)
                //.config("spark.sql.warehouse.dir", "hdfs://")
                .config("spark.hive.metastore.uris", thriftServer)
                .config("spark.sql.parquet.writeLegacyFormat", "true")
                .enableHiveSupport()
                .getOrCreate();

        return sparkSession;
    }

}
