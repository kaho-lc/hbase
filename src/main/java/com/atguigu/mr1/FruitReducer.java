package com.atguigu.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-16
 * @time 9:08
 */
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {

  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    // 1.遍历values
    for (Text value : values) {

      // 2.获取每一行的数据
      String[] split = value.toString().split("\t");

      // 3.构建Put对象
      Put put = new Put(Bytes.toBytes(split[0]));

      // 4.Put对象赋值
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));

      // 5.写出
      context.write(NullWritable.get(), put);
    }
  }
}
