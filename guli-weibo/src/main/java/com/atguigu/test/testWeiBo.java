package com.atguigu.test;

import com.atguigu.constants.Constants;
import com.atguigu.dao.HbaseDAO;
import com.atguigu.utils.HbaseUtil;

import java.io.IOException;

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-18
 * @time 20:46
 */
public class testWeiBo {

  public static void main(String[] args) throws IOException, InterruptedException {

    // 1.初始化
    init();

    // 1001发布微博
    HbaseDAO.publishWeiBo("1001", "赶紧下课吧！");

    // 1002关注1001和1003
    HbaseDAO.addAttends("1002", "1001", "1003");

    // 获取1002初始化页面
    HbaseDAO.getInit("1002");
    System.out.println("****************111*****************");

    // 1003发布3条微博，同时1001发布2条
    HbaseDAO.publishWeiBo("1003", "谁说的赶紧下课？");
    Thread.sleep(10);
    HbaseDAO.publishWeiBo("1001", "我没说话啊。。。");
    Thread.sleep(10);
    HbaseDAO.publishWeiBo("1003", "那谁说的啊？");
    Thread.sleep(10);
    HbaseDAO.publishWeiBo("1001", "反正不是我说的。。。");
    Thread.sleep(10);
    HbaseDAO.publishWeiBo("1003", "你们爱咋咋地吧。。。");

    // 获取1002初始化页面
    HbaseDAO.getInit("1002");
    System.out.println("****************222*****************");

    // 1002取关1003
    HbaseDAO.deleteAttends("1002", "1003");

    // 获取1002初始化页面
    HbaseDAO.getInit("1002");
    System.out.println("****************333*****************");

    // 1002再次关注1003
    HbaseDAO.addAttends("1002", "1003");

    // 1001微博详情
    HbaseDAO.getWeiBo("1001");
  }

  public static void init() {

    try {
      // 1.創建命名空間
      HbaseUtil.createNameSpace(Constants.NAMESPACE);

      // 2.创建微博内容表
      HbaseUtil.createTable(
          Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONS, Constants.CONTENT_TABLE_CF);

      // 3.创建用户关系表
      HbaseUtil.createTable(
          Constants.RELATION_TABLE,
          Constants.RELATION_TABLE_VERSION,
          Constants.RELATION_TABLE_cf1,
          Constants.RELATION_TABLE_cf2);

      // 4.创建收件箱表
      HbaseUtil.createTable(
          Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSION, Constants.INBOX_TABLE);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
