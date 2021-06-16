package com.atguigu.m2;

import com.atguigu.mr1.FruitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-16
 * @time 10:10
 */
public class Fruit2Driver implements Tool {
  private Configuration configuration = null;

  @Override
  public int run(String[] args) throws Exception {
    // 1.获取job对象
    Job job = Job.getInstance(configuration);

    // 2.设置主类路径
    job.setJarByClass(Fruit2Driver.class);

    // 3.设置mapper和k,v类型
    TableMapReduceUtil.initTableMapperJob(
        args[0], new Scan(), Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class, job);

    // 4.设置Reducer和输出的表
    TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);

    // 5.提交任务
    boolean result = job.waitForCompletion(true);

    return result ? 0 : 1;
  }

  @Override
  public void setConf(Configuration conf) {
    configuration = conf;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  public static void main(String[] args) {

    try {
      Configuration configuration = new Configuration();
      ToolRunner.run(configuration, new Fruit2Driver(), args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
