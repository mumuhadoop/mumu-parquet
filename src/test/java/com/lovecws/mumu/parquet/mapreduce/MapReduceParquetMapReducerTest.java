package com.lovecws.mumu.parquet.mapreduce;

import com.lovecws.mumu.parquet.ParquetConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-18 11:16
 */
public class MapReduceParquetMapReducerTest {

    @Test
    public void mapreduce() {
        String inputPath = ParquetConfiguration.HDFS_URI + "//parquet/mapreduce/input";
        String outputPath = ParquetConfiguration.HDFS_URI + "//parquet/mapreduce/output" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
        try {
            MapReduceParquetMapReducer.main(new String[]{inputPath, outputPath});
            DistributedFileSystem distributedFileSystem = new ParquetConfiguration().distributedFileSystem();
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path(outputPath));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus);
            }
            distributedFileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
