package com.atguigu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-14
 * @time 19:23
 */

/*
 * DDL
 *       1.创建命名空间
 *       2.判断表是否存在
 *       3.创建表
 *       4.删除表
 * DML
 *       5.插入数据
 *       6.查数据(get)
 *       7.查数据(scan)
 *       8.删除数据
 *
 *
 *
 *
 * */
public class TestAPI {
  private static Connection conn = null;
  private static Admin admin = null;

  /** 静态代码块，用来存放配置信息 */
  static {
    try {
      // 1.获取配置文件信息
      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

      // 2.获取连接对象
      conn = ConnectionFactory.createConnection(conf);

      // 3.创建Admin对象
      admin = conn.getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    // 1.测试表是否存在
    System.out.println(isTableExist("stu3"));

    // 2.创建表测试
    createTable("stu6", "info1", "info2");

    // 3.删除表测试
    dropTable("stu3");

    // 4.创建命名空间测试
    createNameSpace("test");

    // 5.插入数据测试
    putData("stu6", "1001", "info1", "name", "zhangsan");
    putData("stu6", "1002", "info1", "name", "zhangsan");

    // 6.获取数据测试(get)
    getDate("stu6", "1002", "info1", "name");

    // 7.获取数据测试(scan)
    scanTable("stu6");

    // 8.测试删除数据
    deleteDate("stu5", "1001", "info1", "name");

    // 关闭资源
    close();
  }

  /** 用来关闭连接 */
  public static void close() {
    if (admin != null) {
      try {
        admin.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * 1.判断表是否存在
   *
   * @param tableName
   * @return
   * @throws IOException
   */
  public static boolean isTableExist(String tableName) throws IOException {

    // 3.判断表是否存在
    boolean exists = admin.tableExists(TableName.valueOf(tableName));

    // 4.关闭资源,调用close静态方法
    close();
    // 5.返回结果
    return exists;
  }

  /**
   * 2.创建表
   *
   * @param tableName
   * @param args
   * @throws IOException
   */
  public static void createTable(String tableName, String... args) throws IOException {

    // 1.判断是否存在列族信息
    if (args.length <= 0) {
      System.out.println("请设置列族信息！");
      return;
    }

    // 2.判断表示否存在
    if (isTableExist(tableName)) {
      System.out.println(tableName + "表已存在！");
      return;
    }

    // 3.创建表描述器
    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

    // 4.循环添加列族信息
    for (String arg : args) {

      // 5.创建列族表述器
      HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(arg);

      // 6.添加列族信息
      hTableDescriptor.addFamily(hColumnDescriptor);
    }

    // 7.创建表
    admin.createTable(hTableDescriptor);
  }

  /**
   * 3.删除表
   *
   * @param tableName
   * @throws IOException
   */
  public static void dropTable(String tableName) throws IOException {

    // 1.判断表是否存在
    if (!isTableExist(tableName)) {
      System.out.println(tableName + "表不存在，无法删除！");
      return;
    }

    // 2.使表下线
    admin.disableTable(TableName.valueOf(tableName));

    // 3.删除表
    admin.deleteTable(TableName.valueOf(tableName));
  }

  /**
   * 4.创建命名空间
   *
   * @param spaceName
   */
  public static void createNameSpace(String spaceName) {
    // 1.创建命名空间描述器
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(spaceName).build();

    // 2.创建命名空间
    try {
      admin.createNamespace(namespaceDescriptor);
    } catch (NamespaceExistException e) {
      System.out.println(spaceName + "命名空间已存在");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 5.向表中插入数据
   *
   * @param tableName 表名
   * @param rowKey 行
   * @param cf 列族
   * @param cn 列名
   * @param value 值
   * @throws IOException
   */
  public static void putData(String tableName, String rowKey, String cf, String cn, String value)
      throws IOException {

    // 1.获取表对象
    Table table = conn.getTable(TableName.valueOf(tableName));

    // 2.创建put对象
    Put put = new Put(Bytes.toBytes(rowKey));

    // 3.给put对象赋值
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));

    // 4.插入数据
    table.put(put);

    // 5.关闭表连接
    table.close();
  }

  /**
   * 6.获取数据(get)
   *
   * @param tableName
   * @param rowKey
   * @param cf
   * @param cn
   * @throws IOException
   */
  public static void getDate(String tableName, String rowKey, String cf, String cn)
      throws IOException {

    // 1.获取表对象
    Table table = conn.getTable(TableName.valueOf(tableName));

    // 2.创建get对象
    Get get = new Get(Bytes.toBytes(rowKey));

    // 2.1指定获取的列族
    // 此步必须指定，否则会返回所有的列族的数据
    //      get.addFamily(Bytes.toBytes(cf));

    // 2.2指定列族和列
    //      get.addColumn(Bytes.toBytes(cf) , Bytes.toBytes(cn));

    // 2.3设置获取数据的版本数
    //      get.setMaxVersions();

    // 3.获取数据
    Result result = table.get(get);

    // 4.解析result,并且打印
    for (Cell cell : result.rawCells()) {

      // 5.打印数据
      System.out.println(
          "CF:"
              + Bytes.toString(CellUtil.cloneFamily(cell))
              + " CN:"
              + Bytes.toString(CellUtil.cloneQualifier(cell))
              + " Value:"
              + Bytes.toString(CellUtil.cloneValue(cell)));
    }

    // 6.关闭表链接
    table.close();
  }

  /**
   * 7.获取数据(scan)
   *
   * @param tableName :表名
   * @throws IOException
   */
  public static void scanTable(String tableName) throws IOException {

    // 1.获取表对象
    Table table = conn.getTable(TableName.valueOf(tableName));

    // 2.创建Scan对象
    Scan scan = new Scan(Bytes.toBytes("1002"));

    // 3.扫描表
    ResultScanner resultScanner = table.getScanner(scan);

    // 4.解析resultScanner
    for (Result result : resultScanner) {

      // 5.解析result并且打印
      for (Cell cell : result.rawCells()) {
        System.out.println(
            "rowKey:"
                + Bytes.toString(CellUtil.cloneRow(cell))
                + " CF:"
                + Bytes.toString(CellUtil.cloneFamily(cell))
                + " CN:"
                + Bytes.toString(CellUtil.cloneQualifier(cell))
                + " Value:"
                + Bytes.toString(CellUtil.cloneValue(cell)));
      }
    }

    // 5.关闭表连接
    table.close();
  }

  public static void deleteDate(String tableName, String rowKey, String cf, String cn)
      throws IOException {

    // 1.获取表连接
    Table table = conn.getTable(TableName.valueOf(tableName));

    // 2.构建删除对象
    Delete delete = new Delete(Bytes.toBytes(rowKey));

    // 2.1设置删除的列
    //    delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

    // 2.2删除指定的列族
    delete.addFamily(Bytes.toBytes(cf));

    // 3.删除数据
    table.delete(delete);

    // 4.关闭表连接
    table.close();
  }
}
