package com.atguigu.mr1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-16
 * @time 9:07
 */
public class FruitMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //只是将数据读取过来写入到hbase表中，因此不需要做任何处理
        context.write(key , value);
    }
}
