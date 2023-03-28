package org.apache.iceberg.spark.actions;

import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;

import java.util.Map;

@Log4j2
public class MigrateTable {
    SparkSession spark;
    Identifier sourceTable;
    Identifier backupTable;
    Map<String, String> properties;

    IcebergMigration icebergMigration;

    public MigrateTable(SparkSession spark,
                        String sourceTable,
                        String backupTableSuffix,
                        Map<String, String> properties) {

        this.spark = spark;
        this.properties = properties;

        String ctx = "migrate target";
        CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
        Spark3Util.CatalogAndIdentifier catalogAndIdent =
                Spark3Util.catalogAndIdentifier(ctx, spark, sourceTable, defaultCatalog);

        this.sourceTable = catalogAndIdent.identifier();
        this.backupTable = Identifier.of(this.sourceTable.namespace(),
                String.format("%s_%s", this.sourceTable.name(), backupTableSuffix));

        icebergMigration = new IcebergMigration(spark, catalogAndIdent.catalog(),
                this.sourceTable,
                backupTable,
                properties);
    }

    public long executeMigration() {

        return icebergMigration.doMigrate();

    }

}
