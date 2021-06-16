package com.atguigu.m2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-16
 * @time 10:10
 */
public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {

    // 1.构建put对象
    Put put = new Put(key.get());

    // 2.获取数据
    for (Cell cell : value.rawCells()) {

      // 3.判断当前的cell是否为“name”列
      if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {

        // 4.给put对象赋值
        put.add(cell);
      }
    }

    // 4.写出
    context.write(key, put);
  }
}
