package com.atguigu.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-18
 * @time 13:05
 */
public class Constants {

  // hbase的配置信息
  public static Configuration configuration = HBaseConfiguration.create();

  // 命名空间
  public static String NAMESPACE = "weibo";

  // 微博内容表
  public static String CONTENT_TABLE = "weibo:content";
  public static String CONTENT_TABLE_CF = "info";
  public static int CONTENT_TABLE_VERSIONS = 1;

  // 用户关系表
  public static String RELATION_TABLE = "weibo:relation";
  public static String RELATION_TABLE_cf1 = "attends";
  public static String RELATION_TABLE_cf2 = "fans";
  public static int RELATION_TABLE_VERSION = 1;

  // 收件箱表
  public static String INBOX_TABLE = "weibo:inbox";
  public static String INBOX_TABLE_CF = "info";
  public static int INBOX_TABLE_VERSION = 2;
}
