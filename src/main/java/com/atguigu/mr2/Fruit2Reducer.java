package com.atguigu.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-16
 * @time 10:10
 */
public class Fruit2Reducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
      throws IOException, InterruptedException {

    // 遍历写出
    for (Put value : values) {
      context.write(NullWritable.get(), value);
    }
  }
}
