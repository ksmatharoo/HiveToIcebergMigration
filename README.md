# HiveToIcebergMigration

This project require hive running on local machine,you can start hive with docker image 
in hiveDocker directory.
Steps to start docker
1. go hive docker dir and run $sudo docker-compose up
2. run $sudo docker ps  //to list the running container and ports
3. check hive thrift server ip address using following commands

   $ sudo docker exec -it hive-metastore /bin/bash

   $ root@hive-metastore:/opt# ifconfig       //use this ip address as thrift server ip in code and test
4. get the nameNode ip address using same commands and add this to /etc/hosts files 
5. browse name node using http://<nameNodeIp>:50070/dfshealth.html#tab-overview


Code provide more control over existing iceberg migration by providing 2 wrapper class
IcebergMigration.java and MigrateTable.java.

Complete migration process is simulated using MigrationHiveToIceberg.java class
which will create a partitioned hive table and call migration on that.



