package com.atguigu.spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}
import org.junit._

import scala.collection.immutable.Nil

/**
 *  Created by chao-pc  on 2020-07-20 15:33
 *
 *  UDF :一进一出
 *  UDAF:多进一出
 *
 */
class CustomFunctionTest {
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
   * UDF（User- Defined Funcation）用户定义函数
   *        ①定义函数
   *        ②注册函数
   *        ③使用
   */
  @Test
  def test1 () : Unit = {

    // sayHello("Curry") ==>" hi,Curry"

    //定义加注册
    sparkSession.udf.register("sayhello",(name:String) => "hi,"+name)

    import sparkSession.implicits._
    val df: DataFrame = sparkSession.read.json("input/employees.json")

    val ds: Dataset[User1] = df.as[User1]

    //自定义的函数需要在sql中使用，需要先创建表
    ds.createTempView("emp") //创建一个临时表

    //执行sql
    sparkSession.sql("select sayhello(name) from emp").show()

  }

  /**
   * 自定义UDAF(User- Defined Aggregation Funcation)用户定义聚合函数
   *  老版本,继承UserDefinedAggregateFunction实现
   *
   *    模拟sun()
   *    ①创建函数
   *    ②注册函数
   *    ③使用函数
   */
  @Test
  def test2 () : Unit = {

    val df: DataFrame = sparkSession.read.json("input/employees.json")

    df.printSchema() //查看df的对象的结构类型

    //创建函数类型
    val mySum = new MySum

    //注册
    sparkSession.udf.register("mySum",mySum)

    //使用
    df.createTempView("emp")

    sparkSession.sql("select mySum(salary) from emp").show()


  }

  /**
   *    Aggregator[IN, BUF, OUT]
   *           IN: 输入的类型   User1
   *           BUF： 缓冲区类型   MyBuffer
   *           OUT：输出的类型  Double
   *
   *           和DataSet配合使用，使用强类型约束！
   *
   *         求平均值：    sum /  count
   */
  @Test
  def test3 () : Unit = {

    import sparkSession.implicits._

    val df: DataFrame = sparkSession.read.json("input/employees.json")

    val ds: Dataset[User1] = df.as[User1]

    //创建UDAF
    val myAvg = new MyAvg

    //将UDAF转换为一个列名
    val aggColumn: TypedColumn[User1, Double] = myAvg.toColumn

    //使用DSL风格查询
    ds.select(aggColumn).show()

  }


}

/**
 * 类似累加器
 */
class MySum extends UserDefinedAggregateFunction{

  //输入的数据的结构信息（类型）
  override def inputSchema: StructType = StructType( StructField("input",DoubleType) :: Nil)

  //buffer: 缓冲区(用于保存途中的临时结果)
  //bufferSchema : 缓存区的类型
  override def bufferSchema: StructType = StructType( StructField("sum",DoubleType) :: Nil)

  //最终返回的结果类型
  override def dataType: DataType = DoubleType

  //是否是确定性的，传入相同的输入是否会总是返回相同的输出
  override def deterministic: Boolean = true

  //初始化缓冲区，赋值zero    MutableAggregationBuffer extends Row
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0)=0.0

  //将输入的数据，进行计算，更新buffle  分区内运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //input ：Row：输入的这一列
    buffer(0) =buffer.getDouble(0) + input.getDouble(0)
  }

  //分区间合并结果，将buffer2的结构累加到buffer1
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = buffer(0)
}



class MyAvg extends Aggregator[User1,MyBuffer,Double]{//[输入的类型，中间值的类型减少，输出的类型]
  //初始化缓冲区 初始sum=0.0，count=0
  override def zero: MyBuffer = MyBuffer(0.0,0)

  //分区内聚合
  override def reduce(b: MyBuffer, a: User1): MyBuffer = {
    //累加buffle的sum
    b.sum +=a.salary
    //累加buffle的count
    b.count += 1
    b
  }

  //分区间聚合，将b2的值聚合到b1上
  override def merge(b1: MyBuffer, b2: MyBuffer): MyBuffer = {
    b1.count+=b2.count
    b1.sum+=b2.sum
    b1
  }

  //返回集合
  override def finish(reduction: MyBuffer): Double = reduction.sum /reduction.count

  //buffer的Encoder类型  样例类，ExpressionEncoder[样例类类型] 或 Encoders.product
  override def bufferEncoder: Encoder[MyBuffer] = ExpressionEncoder[MyBuffer]

  //最终返回结果的Encoder类型
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble //系统提供的返回Double类型
}


//每个样例类都会实现Product和Serializable
case class MyBuffer(var sum:Double,var count:Int)
