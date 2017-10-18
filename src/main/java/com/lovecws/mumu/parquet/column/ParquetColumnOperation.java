package com.lovecws.mumu.parquet.column;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 将数据写入到parquet文件中
 * @date 2017-10-18 10:25
 */
public class ParquetColumnOperation {

    private static final MessageType schema = MessageTypeParser.parseMessageType(
            "message Pair {\n" +
                    " required binary left (UTF8);\n" +
                    " required binary right (UTF8);\n" +
                    "}");

    /**
     * 写入parquet数据
     *
     * @param parquetFile parquet文件路径
     */
    public void write(String parquetFile) {
        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup()
                .append("left", "L")
                .append("right", "R");

        Configuration conf = new Configuration();
        Path path = new Path(parquetFile);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = null;
        try {
            writer = new ParquetWriter<Group>(path, writeSupport,
                    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetProperties.WriterVersion.PARQUET_1_0, conf);
            writer.write(group);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 读取parquet数据
     *
     * @param parquetFile parquet文件路径
     */
    public void read(String parquetFile) {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = null;
        try {
            reader = new ParquetReader<Group>(new Path(parquetFile), readSupport);

            Group group = null;
            while ((group = reader.read()) != null) {
                System.out.println(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
