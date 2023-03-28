package org.apache.iceberg;

import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class IceCustomUtils {

    SparkSession session;
    String basePath;
    String newBasePath;

    public IceCustomUtils(SparkSession session, String basePath, String newBasePath) {
        this.session = session;
        this.basePath = basePath;
        this.newBasePath = newBasePath;
    }

    public void checkDataFile(DataFile expected, DataFile actual) {
        Assert.assertEquals("Path must match", expected.path(), actual.path());
        Assert.assertEquals("Format must match", expected.format(), actual.format());
        Assert.assertEquals("Record count must match", expected.recordCount(), actual.recordCount());
        Assert.assertEquals("Size must match", expected.fileSizeInBytes(), actual.fileSizeInBytes());
        Assert.assertEquals(
                "Record value counts must match", expected.valueCounts(), actual.valueCounts());
        Assert.assertEquals(
                "Record null value counts must match",
                expected.nullValueCounts(),
                actual.nullValueCounts());
        Assert.assertEquals(
                "Record nan value counts must match", expected.nanValueCounts(), actual.nanValueCounts());
        Assert.assertEquals("Lower bounds must match", expected.lowerBounds(), actual.lowerBounds());
        Assert.assertEquals("Upper bounds must match", expected.upperBounds(), actual.upperBounds());
        Assert.assertEquals("Key metadata must match", expected.keyMetadata(), actual.keyMetadata());
        Assert.assertEquals("Split offsets must match", expected.splitOffsets(), actual.splitOffsets());
        Assert.assertEquals("Sort order id must match", expected.sortOrderId(), actual.sortOrderId());

        checkStructLike(expected.partition(), actual.partition());
    }

    public void checkStructLike(StructLike expected, StructLike actual) {
        Assert.assertEquals("Struct size should match", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(
                    "Struct values must match", expected.get(i, Object.class), actual.get(i, Object.class));
        }
    }

    public void readMetadata(Table table) throws IOException {


        ////////////////////////metadata.json/////////////////////////////////////////
        HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();
        TableMetadata tableMetadata = ops.current();
        String currentMetadataLocation = ops.currentMetadataLocation();

        TableMetadata read = TableMetadataParser.read(table.io(), currentMetadataLocation);

        //todo path need to updated at these 2 places for metadata
        List<TableMetadata.MetadataLogEntry> metadataLogEntries = read.previousFiles();
        List<Snapshot> snapshots = read.snapshots();

        TableMetadata.Builder update = TableMetadata.buildFrom(tableMetadata);


        ////////////////////////////////////////////////////////

        FileIO fileIO = table.io();
        String manifestListLocation = table.currentSnapshot().manifestListLocation();
        List<ManifestFile> manifestList = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
        String manifestListFileName = Paths.get(manifestListLocation).getFileName().toString();


        List<ManifestFile> updatedManifestFileList = new ArrayList<>();
        int totalDataFilesInCurrentSnapshot = 0;

        //looping manifestList to get all manifestFiles
        for (ManifestFile manifestFile : manifestList) {
            ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifestFile, table.io());

            String manifestFileName = Paths.get(manifestFile.path()).getFileName().toString();

            List<DataFile> dataFiles = Lists.newArrayList();
            CloseableIterator<DataFile> iterator = manifestReader.iterator();

            //looping manifestFile to get all dataFiles
            while (iterator.hasNext()) {
                DataFile dataFile = iterator.next();
                dataFiles.add(dataFile.copy());
            }
            totalDataFilesInCurrentSnapshot += dataFiles.size();

            ManifestFile updatedManifestFile = writeManifest(manifestFileName, table, table.currentSnapshot().snapshotId(),
                    dataFiles.toArray(new DataFile[0]));

            updatedManifestFileList.add(updatedManifestFile);
        }
        System.out.println("totalDataFilesInCurrentSnapshot" + totalDataFilesInCurrentSnapshot);

        //write manifestList with updated path

        try(ManifestListWriter writer =
                ManifestLists.write(2, Files.localOutput(newBasePath + manifestListFileName),
                        table.currentSnapshot().snapshotId(),//snapshotId,
                        table.currentSnapshot().parentId(),
                        table.currentSnapshot().sequenceNumber())) {
            writer.addAll(updatedManifestFileList);
        }

        System.out.println("end");

    }


    public ManifestFile writeManifest(String outputFileName, Table table, Long snapshotId, DataFile... files) throws IOException {
        int formatVersion = 2;


        File manifestFile = new File(newBasePath + outputFileName);
        OutputFile outputFile = table.io().newOutputFile(manifestFile.getCanonicalPath());

        ManifestWriter<DataFile> writer =
                ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
        try {
            for (DataFile file : files) {
                writer.add(updateBasePath(file));
            }
        } finally {
            writer.close();
        }

        return writer.toManifestFile();
    }

    public DataFile updateBasePath(DataFile dataFile) {
        if (dataFile instanceof GenericDataFile) {
            GenericDataFile gdf = (GenericDataFile) dataFile;

            Metrics metrics = new Metrics(gdf.recordCount(), gdf.columnSizes(), gdf.valueCounts(),
                    gdf.nullValueCounts(), gdf.nanValueCounts(), gdf.lowerBounds(), gdf.upperBounds());

            PartitionData partitionData = new PartitionData(gdf.getSchema());

            String path = gdf.path().toString().replace(basePath, newBasePath);

            return new GenericDataFile(gdf.specId(), path, gdf.format(), partitionData, gdf.fileSizeInBytes(),
                    metrics, gdf.keyMetadata(), gdf.splitOffsets(), gdf.sortOrderId());
        }
        return dataFile;

    }
}

