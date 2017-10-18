package com.lovecws.mumu.parquet.benchmark;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-18 15:36
 */
public class JMHBenchmarkTest {
    private static final MessageType schema = MessageTypeParser.parseMessageType(
            "message SinaFinanceNews {\n" +
                    " required binary htitle (UTF8);\n" +
                    " required binary keywords (UTF8);\n" +
                    " required binary description (UTF8);\n" +
                    " required binary url (UTF8);\n" +
                    " required binary sumary (UTF8);\n" +
                    " required binary content (UTF8);\n" +
                    " required binary logo (UTF8);\n" +
                    " required binary title (UTF8);\n" +
                    " required binary pubDate (UTF8);\n" +
                    " required binary mediaName (UTF8);\n" +
                    " required binary mediaUrl (UTF8);\n" +
                    " required binary category (UTF8);\n" +
                    " required binary type (UTF8);\n" +
                    "}");

    private static final String CLASSPATH_PATH = JMHBenchmarkTest.class.getResource("/").getPath();
    private static final String PARQUETPATH = CLASSPATH_PATH + "financeNews" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss") + ".parquet";

    @Test
    public void write() {
        ParquetWriter<Group> writer = null;
        Configuration configuration = new Configuration();
        try {
            GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
            GroupWriteSupport.setSchema(schema, configuration);
            writer = new ParquetWriter<Group>(new Path(PARQUETPATH), groupWriteSupport,
                    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetProperties.WriterVersion.PARQUET_1_0, configuration);
            GroupFactory groupFactory = new SimpleGroupFactory(schema);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(CLASSPATH_PATH + "financeNews.json")));
            String readline = null;
            while ((readline = bufferedReader.readLine()) != null) {
                Map map = JSON.parseObject(readline, Map.class);
                System.out.println(map);
                Group group = groupFactory.newGroup()
                        .append("htitle", String.valueOf(map.get("htitle")))
                        .append("keywords", String.valueOf(map.get("keywords")))
                        .append("description", String.valueOf(map.get("description")))
                        .append("url", String.valueOf(map.get("url")))
                        .append("sumary", String.valueOf(map.get("sumary")))
                        .append("content", String.valueOf(map.get("content")))
                        .append("logo", String.valueOf(map.get("logo")))
                        .append("title", String.valueOf(map.get("title")))
                        .append("pubDate", String.valueOf(map.get("pubDate")))
                        .append("mediaName", String.valueOf(map.get("mediaName")))
                        .append("mediaUrl", String.valueOf(map.get("mediaUrl")))
                        .append("category", String.valueOf(map.get("category")))
                        .append("type", String.valueOf(map.get("type")));
                writer.write(group);
            }
            writer.close();
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}