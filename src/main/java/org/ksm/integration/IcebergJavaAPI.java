package org.ksm.integration;

import org.apache.iceberg.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.data.Record;


import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IcebergJavaAPI {

    public static void fun(SparkSession spark){


        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(spark.sparkContext().hadoopConfiguration());  // Configure using Spark's Hadoop configuration

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", "...");
        properties.put("uri", "...");

        catalog.initialize("hive", properties);

        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .identity("level")
                .build();

        TableIdentifier name = TableIdentifier.of("logging", "logs");
        Table table = catalog.createTable(name, schema, spec);

        //or to load an existing table, use the following line
        //Table table = catalog.loadTable(name);

        //TableScan scan = table.newScan();
        //TableScan filteredScan = scan.filter(Expressions.equal("id", 5));

        TableScan scan = table.newScan()
                .filter(Expressions.equal("id", 5))
                .select("id", "data");

        Schema projection = scan.schema();
        Iterable<CombinedScanTask> tasks = scan.planTasks();

        //row level
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
        scanBuilder.where(Expressions.equal("id", 5));

        CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.lessThan("id", 5))
                .build();

        //update schema
        table.updateSchema()
                .addColumn("count", Types.LongType.get())
                .commit();

        Transaction t = table.newTransaction();

        // commit operations to the transaction
        Expression filter = Expressions.equal("id", 5);
        t.newDelete().deleteFromRowFilter(filter).commit();

        //SparkDataFile sparkDataFile = new SparkDataFile();

        t.newAppend().appendFile(null).commit();

        // commit all the changes to the table
        t.commitTransaction();
    }

    public static ManifestFile writeManifest(Long snapshotId, Table table, DataFile... files) throws IOException {
        File manifestFile = new File("input.m0.avro");
        //Assert.assertTrue(manifestFile.delete());
        OutputFile outputFile = table.io().newOutputFile(manifestFile.getCanonicalPath());

        int formatVersion = 2;
        ManifestWriter<DataFile> writer =
                ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
        try {
            for (DataFile file : files) {
                writer.add(file);
            }
        } finally {
            writer.close();
        }
        return writer.toManifestFile();
    }

}
