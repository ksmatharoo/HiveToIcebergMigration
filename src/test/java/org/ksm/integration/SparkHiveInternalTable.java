package org.ksm.integration;

import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class SparkHiveInternalTable extends TestCase {

    @Test
    public void test(){

        System.out.println("");

        try {
            String warehousePath = System.getProperty("user.dir") + "/data/";

            String thriftServer = "thrift://172.18.0.5:9083";

            //String tableName = "decimal_test_hive_false";
            String tableName = "decimal_test_k_1";
            String dbname = "default";
            String partitionKeys = "name";
            String basePath = warehousePath + tableName;

            SparkSession session = Utils.getSparkSession(warehousePath, thriftServer);
            session.sql("SELECT * FROM my_parquet_hive_table").show();


            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("name", DataTypes.StringType, true),
                    DataTypes.createStructField("decimalColumn7", DataTypes.createDecimalType(7, 2), true),
                    DataTypes.createStructField("decimalColumn15", DataTypes.createDecimalType(15, 2), true),
                    DataTypes.createStructField("decimalColumn38", DataTypes.createDecimalType(38, 2), true)
            });

            // Sample data
            List<Row> data = Arrays.asList(
                    RowFactory.create("John", new BigDecimal("12345.67"), new BigDecimal("1234512345123.67"), new BigDecimal("123451234512345123451234512345.67")),
                    RowFactory.create("John", new BigDecimal("98764.54"), new BigDecimal("1234512345123.67"), new BigDecimal("523451234512345123451234512345.67"))
            );

            // Create a DataFrame with the specified schema and data
            Dataset<Row> dataset = session.createDataFrame(data, schema);


            //session.sql("create database if not exists " + dbname).show(false);

            dataset.createOrReplaceTempView("my_temp_view");

            // Execute Hive SQL to create a Hive table from the DataFrame
            // Execute Hive SQL to create a Hive table from the DataFrame with Parquet format
            session.sql("CREATE TABLE IF NOT EXISTS my_parquet_hive_table " +
                    "USING PARQUET " +
                    "AS SELECT * FROM my_temp_view");

            // Show the data in the Hive table
            session.sql("SELECT * FROM my_parquet_hive_table").show();


            System.out.println();
            //dataset.write().insertInto("default."+tableName);

            // Register DataFrame as a temporary view
            //dataset.createOrReplaceTempView("my_temp_view");

            // Execute Hive SQL to create a Hive table
            //session.sql("CREATE TABLE IF NOT EXISTS my_hive_table AS SELECT * FROM my_temp_view");

            // Show the data in the Hive table
            //session.sql("SELECT * FROM my_hive_table").show();
            System.out.println("");
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}