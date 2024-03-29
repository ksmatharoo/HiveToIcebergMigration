package org.ksm.integration;

import junit.framework.TestCase;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
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


@Log4j2
public class SparkUtilsTest extends TestCase {

    @Test
    public void testDecimal() {
        try {
            String warehousePath = System.getProperty("user.dir") + "/data/";
            String thriftServer = "thrift://172.18.0.5:9083";

            String tableName = "decimal_test_false";
            String dbname = "testksm";
            String partitionKeys = "name";
            String basePath = warehousePath + tableName;

            SparkSession session = Utils.getSparkSession(warehousePath, thriftServer);
            session.sql("show tables").show(false);

            session.sql("select * from testksm." + tableName).show(false);

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
            session.sql("create database if not exists " + dbname).show(false);
            MigrationHiveToIceberg.createAndLoadHiveTable(session, dataset, tableName, dbname, basePath, partitionKeys);
            System.out.println("");
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            if (e instanceof AlreadyExistsException) {
                log.warn(e.toString());
            } else {
                throw new RuntimeException(e);
            }
        }
    }

}