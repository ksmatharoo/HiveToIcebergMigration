# HiveToIcebergMigration

This project require hive running on local machine,you can start hive with docker image 
in hiveDocker directory.

Code provide more control over existing iceberg migration by providing 2 wrapper class
IcebergMigration.java and MigrateTable.java.

Complete migration process is simulated using MigrationHiveToIceberg.java class
which will create a partitioned hive table and call migration on that.



