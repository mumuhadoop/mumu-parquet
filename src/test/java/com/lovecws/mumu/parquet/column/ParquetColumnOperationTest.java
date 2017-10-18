package com.lovecws.mumu.parquet.column;

import com.lovecws.mumu.parquet.ParquetConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-18 10:43
 */
public class ParquetColumnOperationTest {

    private ParquetColumnOperation parquetColumnOperation = new ParquetColumnOperation();

    @Test
    public void write() {
        parquetColumnOperation.write(ParquetConfiguration.HDFS_URI + "/parquet/column.parquet");
    }

    @Test
    public void read() {
        //parquetColumnOperation.read(ParquetConfiguration.HDFS_URI + "/parquet/column.parquet");
        parquetColumnOperation.read(ParquetConfiguration.HDFS_URI + "/parquet/mapreduce/output20171018112005/part-m-00000.parquet");
    }
}
