package com.lovecws.mumu.parquet.avro;

import com.lovecws.mumu.parquet.ParquetConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-18 10:56
 */
public class AvroParquetOperationTest {

    private AvroParquetOperation avroParquetOperation = new AvroParquetOperation();

    @Test
    public void write() {
        //avroParquetOperation.write(ParquetConfiguration.HDFS_URI + "/parquet/avro.parquet");
        avroParquetOperation.write("file:////E:\\avro.parquet");
    }

    @Test
    public void read() {
        avroParquetOperation.read(ParquetConfiguration.HDFS_URI + "/parquet/avro.parquet");
    }
}
