package org.ksm.integration;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.actions.SnapshotTableSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iceberg.expressions.Expressions.*;


public class SparkUtils {

    public static Dataset<Row> readCSVFile(SparkSession spark,String path){

        Dataset<Row> csv = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/employee.csv");

        csv = csv.withColumn("HIRE_DATE",
                functions.to_date(new Column("HIRE_DATE"), "dd-MMM-yy"));

        return csv;

    }

    /**
     * all column names converted to lower case
     * **/
    public static Dataset<Row> readCSVFileWithoutDate(SparkSession spark,String path){

        Dataset<Row> csv = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/employee.csv");

        List<Column> columnList = Arrays.stream(csv.schema().fields()).map(
                f -> new Column(f.name().toLowerCase())
        ).collect(Collectors.toList());

        return csv.select(columnList.toArray(new Column[0])).withColumn("department_id",
                new Column("department_id").cast("String"));

    }

    public static void updatePartition(HiveCatalog catalog,boolean bYearToMonth,boolean bMonthToYear ,
                                       TableIdentifier identifier, SparkSession spark ){
        Table table = catalog.loadTable(identifier);

        if(bYearToMonth) {
            table.updateSpec().removeField(year("HIRE_DATE"))
                    .addField(month("HIRE_DATE"))
                    .commit();
        }
        if(bMonthToYear) {
            table.updateSpec().removeField(month("HIRE_DATE"))
                    .addField(year("HIRE_DATE"))
                    .commit();

            SparkActions.get(spark).rewriteDataFiles(table).execute();
        }
    }


    /**
     * Migrate from hive to iceberg
     * 1. create a new iceberg table with business_date/someDateColumn as partition column using transformation
     *    as identity
     *
     * 2. use addFiles from path iceberg to add all files to table
     *
     * 3. now iceberg table has business_date as partition column, but it's not present in parquet files
     *
     * 4. update iceberg table partitionSpec to month(business_date),
     *    update iceberg table partitionSpec to remove business_date column
     *    and do rewrite data files, this step will
     *    create new parquet files which will be having business_date in it,
     *
     * 5. done month level compacted table ready
     *
     *        table.updateSpec()
     *        .addField(month("business_date"))
     *        .removeField("business_date")
     *        .commit();
     *
     *         SparkActions.get(spark).rewriteDataFiles(table).execute();
     *
     */

    /**
     * iceberg spark testing
     ***/
    public static void main(String[] args) throws Exception {

        boolean bYearToMonth = false;
        boolean bMonthToYear = false;
        boolean bWriteData = false;
        boolean bAddFiles = true;
        boolean bCustomManifestWrite = false;
        String tableName = "employee14";
        String namespace = "default";
        String wareHousePath = "/home/ksingh/ksingh/IcebergTest/data";


        SparkSession spark = Utils.getSparkSession();
        //spark.sparkContext().setLogLevel("ALL");


        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(spark.sparkContext().hadoopConfiguration());


        spark.sql("show tables").show(false);


        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", wareHousePath);
        properties.put("uri", "thrift://172.19.0.5:9083");


        catalog.initialize("hive", properties);

        Dataset<Row> inputFile = readCSVFile(spark,null);
        Schema iceSchema = SparkSchemaUtil.convert(inputFile.schema());

        //inputFile.write().partitionBy("HIRE_DATE").mode(SaveMode.Overwrite).parquet("/home/ksingh/ksingh/IcebergTest/data/parquetWrite");

        String basePath = wareHousePath;
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);

        if (!catalog.tableExists(identifier)) {
            String location = basePath + "/" + tableName;

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("format-version", "2");

            PartitionSpec spec = PartitionSpec.builderFor(iceSchema)
                    .identity("HIRE_DATE")
                    .build();

            Table table = catalog.createTable(identifier, iceSchema, spec, location, tableProperties);
            System.out.println("table created at : " + table.location());
        } else {

            // add files from path to iceberg table
            //spark.sql("CALL spark_catalog.system.add_files(table => 'default.employee5',source_table => '`parquet`.`/home/ksingh/ksingh/IcebergTest/data/parquetWrite`')").show(false);

            Table table = catalog.loadTable(identifier);

            //test utils
            if(bCustomManifestWrite) {
                new IceCustomUtils(spark, "/home/ksingh/ksingh/IcebergTest/data/employee3/",
                        "/home/ksingh/ksingh/IcebergTest/src/main/resources/avro/").readMetadata(table);
            }

            if(bYearToMonth || bMonthToYear) {
                updatePartition(catalog, bYearToMonth, bMonthToYear, identifier, spark);
            }
            //Transaction t = table.newTransaction();
            // commit operations to the transaction
            //t.newDelete().deleteFromRowFilter(Expressions.equal("id", 5)).commit();
            //t.newAppend().appendFile(data).commit();
            // commit all the changes to the table
            //t.commitTransaction();
        }

        if(bAddFiles){

            Table table = catalog.loadTable(identifier);
            String query = String.format("CALL spark_catalog.system.add_files(table => 'default.%s'," +
                    "source_table => '`parquet`.`/home/ksingh/ksingh/IcebergTest/data/parquetWrite`')",tableName);

            spark.sql(query).show(false);

            table.refresh();

            table.updateSpec()
                    .addField(year("HIRE_DATE"))
                    .removeField("HIRE_DATE")
                    .commit();

            /*table.updateSpec()
                    .removeField("HIRE_DATE")
                    .commit();*/

            SparkActions.get(spark).rewriteDataFiles(table).
                    filter(Expressions.lessThan("HIRE_DATE", "2003-01-01")).execute();



            SparkActions.get(spark).migrateTable("").
                    tableProperties(null).
                    dropBackup().
                    execute();
        }


        if (bWriteData) {
            for(int i=0;i<10;i++) {
                    inputFile.writeTo(tableName).append();
                    //SnapshotTableSparkAction snapshotTableSparkAction = SparkActions.get().snapshotTable("");
                    //snapshotTableSparkAction.execute();
                }
        }

        spark.sql("show tables").show(false);
        System.out.println();

        //Thread.sleep(120000L);

        System.out.println("");

    }

}
