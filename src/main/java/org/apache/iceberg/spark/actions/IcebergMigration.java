package org.apache.iceberg.spark.actions;

import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;

import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import scala.Some;

import java.util.Map;


@Log4j2
class IcebergMigration extends MigrateTableSparkAction {

    SparkSession spark;
    Identifier sourceTable;
    Identifier backupTable;
    Map<String,String> properties;

    Identifier tempIdentifier;

    /**
     *  this function convert a source table(Hive) to iceberg table and original table will be renamed as backUpTable
     *
     *   source table :- sg.delimitedTxSmokeTest
     *   backupTable : sg.delimitedTxSmokeTest_backup_
     *
     * **/
    IcebergMigration(SparkSession spark, CatalogPlugin sourceCatalog, Identifier tableIdent,
                     Identifier backupTable,
                     Map<String,String> properties) {
        super(spark, sourceCatalog, tableIdent);
        this.spark = spark;
        this.sourceTable = tableIdent;
        this.backupTable = backupTable;
        this.properties = properties;
    }

    /***
     * this function will create an iceberg staging table following rule :-
     * icebergTempTable will be  sourceTable_<timestampInMilliseconds>
     * and once metadata is written it will be renamed to sourceTable
     * ****/
    @Override
    protected StagedSparkTable stageDestTable() {
        try {
            this.self().setProperties(properties);
            Map<String, String> props = this.destTableProps();

            V1Table v1Table = loadV1TableSource(this.sourceTable);

            StructType schema = v1Table.schema();
            Transform[] partitioning = v1Table.partitioning();

            String tempTableName =  String.format("%s_%d",this.destTableIdent().name(), System.currentTimeMillis());

            tempIdentifier = Identifier.of(this.destTableIdent().namespace(),tempTableName);
            log.info("Temp iceberg Table name :{}",tempIdentifier.toString());

            return (StagedSparkTable)this.destCatalog().stageCreate(tempIdentifier, schema, partitioning, props);
        } catch (NoSuchNamespaceException var4) {
            throw new org.apache.iceberg.exceptions.NoSuchNamespaceException("Cannot create table %s as the namespace does not exist", new Object[]{this.destTableIdent()});
        } catch (TableAlreadyExistsException var5) {
            throw new AlreadyExistsException("Cannot create table %s as it already exists", new Object[]{this.destTableIdent()});
        }
    }

    protected V1Table loadV1TableSource(Identifier table){

        CatalogPlugin defaultCatalog = this.self().sourceCatalog();

        this.self().v1SourceTable();

        TableCatalog tableCatalog = this.checkSourceCatalog(defaultCatalog);
        V1Table v1Table = null;
        try {
             v1Table = (V1Table) tableCatalog.loadTable(table);
        } catch (NoSuchTableException var5) {
            throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot not find source table '%s'", new Object[]{table});
        } catch (ClassCastException var6) {
            throw new IllegalArgumentException(String.format("Cannot use non-v1 table '%s' as a source", table), var6);
        }
        return v1Table;
    }

    protected boolean renameTable(Identifier sourceTableIdentifier,Identifier targetTableIdentifier) {
        boolean renamed = false;
        try {
            log.info("Renaming {} from {}", sourceTableIdentifier, targetTableIdentifier);
            this.destCatalog().renameTable(sourceTableIdentifier, targetTableIdentifier);
            log.info("Renamed {} from {}", sourceTableIdentifier, targetTableIdentifier);
            renamed = true;
        } catch (NoSuchTableException var2) {
            log.error("Cannot rename the sourceTableIdentifier, the sourceTable {} cannot be found", sourceTableIdentifier, var2);
        } catch (TableAlreadyExistsException var3) {
            log.error("Cannot rename the sourceTableIdentifier {}, as table with the target name {} already exists.",
                    sourceTableIdentifier,targetTableIdentifier, var3);
        }
        return renamed;
    }

    /**
     *  this function convert a source table(Hive) to iceberg table and original table will be renamed as backUpTable
     *
     *   source table :- sg.delimitedTxSmokeTest
     *   backupTable : sg.delimitedTxSmokeTest_backup_
     *
     * **/
    public long doMigrate() {

        log.info("Starting the migration of {} to Iceberg", sourceTable);

        StagedSparkTable stagedTable = null;
        /**
         * icebergTempTable will be  sourceTable_<timestampInMilliseconds>
         * and once metadata is written it will be renamed to sourceTable
         * */
        Table icebergTempTable;
        boolean threw = true;
        boolean isRenamedOrigToBkpTable = false;
        boolean isRenamedTempToOrigTable = false;
        try {
            log.info("Staging a new Iceberg table {}", destTableIdent());
            stagedTable = stageDestTable();
            icebergTempTable = stagedTable.table();

            log.info("Ensuring {} has a valid name mapping", destTableIdent());
            ensureNameMappingPresent(icebergTempTable);

            //Some<String> backupNamespace = Some.apply(backupIdent.namespace()[0]);
            //TableIdentifier v1BackupIdent = new TableIdentifier(backupIdent.name(), backupNamespace);

            Some<String> sourceNamespace = Some.apply(sourceTable.namespace()[0]);
            TableIdentifier v1SourceIdent = new TableIdentifier(sourceTable.name(), sourceNamespace);

            String stagingLocation = getMetadataLocation(icebergTempTable);
            log.info("Generating Iceberg metadata for {} at location {}", destTableIdent(), stagingLocation);
            SparkTableUtil.importSparkTable(spark(), v1SourceIdent, icebergTempTable, stagingLocation);

            log.info("Committing staged changes to {}", destTableIdent());
            stagedTable.commitStagedChanges();
            threw = false;

            //rename originalTable name to backupTable name
            isRenamedOrigToBkpTable = renameTable(sourceTable, backupTable);
            //rename icebergTable to originalTable name
            isRenamedTempToOrigTable = renameTable(tempIdentifier, sourceTable);
        } finally {
            if (threw) {
                log.error(
                        "Failed to perform the migration, aborting table creation and restoring the original table");

                //restoreSourceTable();

                if (stagedTable != null) {
                    try {
                        stagedTable.abortStagedChanges();
                    } catch (Exception abortException) {
                        log.error("Cannot abort staged changes", abortException);
                    }
                }
            } else if(!isRenamedOrigToBkpTable || !isRenamedTempToOrigTable) {
                if(!isRenamedOrigToBkpTable) {
                    log.info("Failed to perform the migration,Original table not renamed to back up.");
                } else {
                    //if(!isRenamedTempToOrigTable) {
                        log.info("Failed to perform the migration,IcebergTempTable to Original Table rename failed,Trying to restore original Hive Table");
                        renameTable(backupTable, sourceTable);

                }
            } else if (false) {//dropBackup) {
                //dropBackupTable();
            }
        }

        Snapshot snapshot = icebergTempTable.currentSnapshot();
        long migratedDataFilesCount =
                Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
        return migratedDataFilesCount;
    }


}
