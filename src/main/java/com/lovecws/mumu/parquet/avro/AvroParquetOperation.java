package com.lovecws.mumu.parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-18 10:50
 */
public class AvroParquetOperation {

    /**
     * 将avro格式的数据写入到parquet文件中
     *
     * @param parquetPath
     */
    public void write(String parquetPath) {
        Schema.Parser parser = new Schema.Parser();
        try {
            Schema schema = parser.parse(AvroParquetOperation.class.getClassLoader().getResourceAsStream("StringPair.avsc"));
            GenericRecord datum = new GenericData.Record(schema);
            datum.put("left", "L");
            datum.put("right", "R");

            Path path = new Path(parquetPath);
            System.out.println(path);
            AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path, schema);
            writer.write(datum);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取parquet文件内容
     *
     * @param parquetPath
     */
    public void read(String parquetPath) {
        AvroParquetReader<GenericRecord> reader = null;
        try {
            reader = new AvroParquetReader<GenericRecord>(new Path(parquetPath));
            GenericRecord result = reader.read();
            System.out.println(result.getSchema());
            while ((result = reader.read()) != null) {
                System.out.println(result);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
