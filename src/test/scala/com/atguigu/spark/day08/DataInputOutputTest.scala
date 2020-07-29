package com.atguigu.spark.day08

/**
 *  Created by chao-pc  on 2020-07-21 9:12
 *  数据的加载和保存
 */

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit._

class DataInputOutputTest {

  //设置master
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
  // 调用SparkSession.builder()
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  @After
  def stop {
    // 关闭session
    sparkSession.stop()
  }

  /**
   * 通用的读取
   *        在spark中默认支持读取和写入的文件格式是parquet格式！
   */
  @Test
  def test1 () : Unit = {

    import sparkSession.implicits._
    //通用方法，可以通过参数改变读取文件的类型
    val df: DataFrame = sparkSession.read.format("json").load("input/employees.json")

    df.show()

    // 专用于读取JSON方法
    val df2: DataFrame = sparkSession.read.json("input/employees.json")

    df2.show()

    val df3: DataFrame = df2.select('name, 'salary + 1000 as("newsalary"))

    //以通用的API，JSON格式输出
    //df3.write.format("json").save("output/out2.json")

    //mode()  overwrite:覆盖重写 append：追加写入 ignore：忽略此文件 error
    df3.write.mode("ignore").json("output/out3.json")

  }

  /**
   *
   */
  @Test
  def test2 () : Unit = {

    //省略建表的过程，直接指定数据源
    sparkSession.sql("select * from json.`input/employees.json`").show()

  }


  /**
   * CSV: 逗号分割的数据集，每行数据的字段都使用，分割
   * TSV：使用\t分割的数据集
   */
  @Test
  def test3 () : Unit = {

    val df: DataFrame = sparkSession.read.format("csv").load("input/mycsv.csv")

    df.show()

    //专用
    val df2: DataFrame = sparkSession.read.csv("input/mycsv.csv")

    df2.show()

  }

  /**
   * 不标准的csv：查看源码设置相应的格式，在读取时，需要添加参数
   *     seq: csv文件中的分隔符
   */
  @Test
  def test4 () : Unit = {

    val df2: DataFrame = sparkSession.read
      .option("sep",":")
      .option("header",true)
      .csv("input/mycsv.csv")

    df2.show()

    df2.write
      .option("sep",",")
      .option("header",false)
      .mode("append").csv("output/out1.csv")

  }

  /**
   * 从关系型数据库中读取数据
   *    ①配置驱动
   */
  @Test
  def test5 () : Unit = {

    val properties = new Properties()

    properties.put("user","root")
    properties.put("password","123456")

    //专用
    val df2: DataFrame = sparkSession.read
      .jdbc("jdbc:mysql://localhost:3306/table_user","table_user",properties)

    //全表查询
    df2.show()

    df2.createTempView("user")  //临时表

    import  sparkSession.sql
    //只查询部分数据
    sql("select * from user where age>20").show()

  }

  /**
   * 参数可以参考JDBCOptions
   */
  @Test
  def test6 () : Unit = {

    //通用
    val df: DataFrame = sparkSession.read
      .option("url","jdbc:mysql://localhost:3306/table_user")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","table_user")  //查询全表
      .format("jdbc").load()

    df.show()


  }

  /**
   * 写
   */
  @Test
  def test7 () : Unit = {

    import sparkSession.implicits._

    val users = List(MyUser("Kobe", 100),MyUser("Lin", 25),MyUser("Wade", 35),MyUser("Tom", 95))

    val ds: Dataset[MyUser] = users.toDS()

    ds.write
      .option("url","jdbc:mysql://localhost:3306/table_user")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","table_name") //选择需要写入的表
      .mode("append") //追加写
      .format("jdbc").save() //jdbc格式

  }


}

case class MyUser(name:String,age:Int)
