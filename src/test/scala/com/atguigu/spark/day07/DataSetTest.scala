package com.atguigu.spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit._

/**
 *  Created by chao-pc  on 2020-07-20 15:04
 */

class DataSetTest {

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
   * RDD转DS
   */
  @Test
  def test1 () : Unit = {

    import sparkSession.implicits._

    val ds: Dataset[Int] = List(1, 2, 3, 4).toDS()

    //df怎么用，ds就怎么用
    ds.show()
  }

  @Test
  def test2 () : Unit = {
    import sparkSession.implicits._

    val list = List(1, 2, 3, 4, 5)

    val rdd: RDD[Int] = sparkSession.sparkContext.makeRDD(list, 2)

    rdd.toDS().show


  }

  /**
   * DS转RDD
   */
  @Test
  def test3 () : Unit = {
    import sparkSession.implicits._

    val ds: Dataset[Int] = List(1, 2, 3, 4, 5).toDS()

    val rdd: RDD[Int] = ds.rdd

    rdd.collect().foreach(println)

  }

  /**
   * DS 转 DF
   */
  @Test
  def test4 () : Unit = {
    import sparkSession.implicits._

    val ds: Dataset[Int] = List(1, 2, 3, 4, 5).toDS()

    val df: DataFrame = ds.toDF()

    df.show()

  }

  /**
   * 借助样例类创建DS(最常使用)
   */
  @Test
  def test5 () : Unit = {
    import sparkSession.implicits._

    val ds: Dataset[User] = List(User("James", 35), User("Curry", 18), User("Wade", 29)).toDS()

    ds.show()

  }

  /**
   * 由DF转DS
   *
   *  Encoder：将一个Java的对象，转为DF或DS结构时，需要使用的编码器！
   *           在创建DS 或 DF时，系统会自动提供隐式的Encoder自动转换！
   *           导入import sparkSession.implicits._
   */
  @Test
  def test6 () : Unit = {
    import sparkSession.implicits._

    //Dataset[Row]
    val df: DataFrame = List(User("James", 35), User("Curry", 18), User("Wade", 29)).toDF()

    //Dataset[Row] => Dataset[User]
    val ds: Dataset[User] = df.as[User]

    ds.show()
  }

  /**
   * DF和DS的区别
   */
  @Test
  def test7 () : Unit = {
    import sparkSession.implicits._

    val df: DataFrame = sparkSession.read.json("input/employees.json")

    //使用df读取数据的第一列 DataSet[Row]
    val ds: Dataset[Int] = df.map(row => row.getInt(0))


    //ds.show()//在运行时出错 ，DS为强类型（在编译时就进行检查），上一行代码中row.getInt(0)提取第一行的第一个字段，但是第一个字段
               // 可能不为Int类型，改为getSting可能会生效，这都需要编译器自己进行转换，所以编译会报错。

    val ds1: Dataset[User1] = df.as[User1]

    val ds2: Dataset[String] = ds1.map(user1 => user1.name)

    ds2.show()
  }

}

case class User(name:String,age:Int)
case class User1(name:String,salary:Double)
