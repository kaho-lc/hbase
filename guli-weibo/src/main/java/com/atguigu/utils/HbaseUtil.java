package com.atguigu.utils;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-18
 * @time 13:00
 */
import com.atguigu.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/** 1.创建命名空间 2.判断表是否存在 3.创建表(三张表) */
public class HbaseUtil {
  /**
   * 1.创建命名空间
   *
   * @param nameSpace
   */
  public static void createNameSpace(String nameSpace) throws IOException {

    // 1.获取connection对象
    Connection conn = ConnectionFactory.createConnection(Constants.configuration);
    // 2.获取Admin对象
    Admin admin = conn.getAdmin();

    // 3.构建命名空间描述器
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

    // 4.创建命名空间
    admin.createNamespace(namespaceDescriptor);

    // 5.关闭资源
    admin.close();
    conn.close();
  }

  /**
   * 2.判断表是否存在
   *
   * @param tableName
   * @return
   * @throws IOException
   */
  private static boolean isTableExist(String tableName) throws IOException {

    // 1.获取connection对象
    Connection conn = ConnectionFactory.createConnection(Constants.configuration);

    // 2.获取Admin对象
    Admin admin = conn.getAdmin();

    // 3.判断表是否存在
    boolean result = admin.tableExists(TableName.valueOf(tableName));

    // 4.关闭资源
    admin.close();
    conn.close();

    // 5.返回结果
    return result;
  }

  public static void createTable(String tableName, int versions, String... args)
      throws IOException {
    // 1.判断是否传入了列族信息
    if (args.length <= 0) {
      System.out.println("未传入列族信息！");
      return;
    }

    // 2.判断表是否为空
    if (isTableExist(tableName)) {
      System.out.println("表已存在");
      return;
    }

    // 3.获取connection对象
    Connection conn = ConnectionFactory.createConnection(Constants.configuration);

    // 4.获取Admin对象
    Admin admin = conn.getAdmin();

    // 5.创建表描述器
    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

    // 6.循环添加列族信息
    for (String arg : args) {
      // 获取列族描述器
      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(arg);

      // 7.设置版本
      hColumnDescriptor.setMaxVersions(versions);

      hTableDescriptor.addFamily(hColumnDescriptor);
    }

    // 8.创建表
    admin.createTable(hTableDescriptor);

    // 9.关闭资源
    admin.close();
    conn.close();
  }
}
