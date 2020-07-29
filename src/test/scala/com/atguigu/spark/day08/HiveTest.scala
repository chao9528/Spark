package com.atguigu.spark.day08

/**
 *  Created by chao-pc  on 2020-07-21 15:01
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit._

class HiveTest {
  
  /**
   * IDEA和外置hive交互
   *  Hive on Spark:  在hive的配置中，有执行引擎，选择 mr,tez,spark
   *                               自己解决兼容性
   *
   *             Spark on Hive:   spark SQL集成hive
   *             Spark SQL:  完全兼容hive sql
   *             Hive SQL:  复合hive要求的语法
   *                         insert into [table] 表名
   *
   */
  System.setProperty("HADOOP_USER_NAME","atguigu") //增加 操作hdfs的权限 也可设置在电脑环境中,hive-site配置中,程序运行配置中
  //原理:SparkSession在构建时,会读取电脑中的环境变量 Environments Variables
  //读取系统变量 System.setProperty ,读每个框架的配置文件

  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("df")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
    .getOrCreate()

  @After
  def stop(): Unit ={
    sparkSession.stop()
  }
  @Test
  def test1 () : Unit = {

    //sparkSession.sql("show tables").show()

    //sparkSession.sql("create table p1(name string)").show() //建表
    //sparkSession.sql("show tables").show()

    //建库需要指明数据库的存储位置config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
    //sparkSession.sql("create database mydb411").show()

    sparkSession.sql("select * from user_visit_action").show()


  }

  @Test
  def test2 () : Unit = {

    val df: DataFrame = sparkSession.sql("select * from user_visit_action where user_id=95")

    df.show()

    //将df中的数据写出
    //重写入表
    //df.write.mode("overwrite").saveAsTable("user_visit_action2")

    //追加写入表  写出的数据的列名和表中的字段名一致即可,顺序可以无所谓
    df.write.mode("append").saveAsTable("user_visit_action2")


  }

  
}
