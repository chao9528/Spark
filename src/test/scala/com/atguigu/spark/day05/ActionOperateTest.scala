package com.atguigu.spark.day05

/**
 *  Created by chao-pc  on 2020-07-15 10:37
 *
 *  1.行动算子和转换算子的区别
 *        转化算子：(transmation Operation)只是将一个RDD转为另一个RDD。是 lazy
 *                  只要算子的返回值是RDD，一定是转换算子！
 *        行动算子：(action Operation)只有执行行动算子，才会触发整个Job的提交！
 *                  只要算子的返回值不是RDD，则就是行动算子！
 *
 * 2.reduce
 * 3.collect
 * 4.count
 * 5.first
 * 6.take
 * 7.takeOrdered
 * 8.aggregate
 * 9.fold
 * 10.countByKey
 * 11.countByValue
 * 12.save相关
 * 13.foreach
 * 14.特殊情况
 */


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.junit._

class ActionOperateTest {
  val conf = new SparkConf().setAppName("My app").setMaster("local[*]")
  val sc = new SparkContext(conf)

  //提供初始方法，完成输出目录的清理
  @Before
  def init(): Unit = {
    //提前删除output fileSystem：本地文件系统
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    //声明结果输出路径
    val path: Path = new Path("output")
    //如果输出目录存在，就删除
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

  //测试完成后关闭
  @After
  def stop() = {
    sc.stop()
  }

  //演示行动算子和转换算子
  @Test
  def test1(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd1: RDD[Int] = rdd.map(x => { //转换算子不能运行job，需要行动算子
      println(x)
      x
    })

    rdd1.saveAsTextFile("output")

    rdd1.collect()

  }

  @Test
  def test2(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd1: RDD[(Int,Int)] = rdd.map(x => { //转换算子不能运行job
      println(x)
      (x,1)
    })

    //val rdd2: RDD[Int] = rdd1.sortBy(x => x) //sortBy在运行时，触发了行动算子，sortBy本身是一个转换算子
    //是RangePartitioner在抽样时触发的，RangePartitioner在抽样时，将抽样的结果进行collect(),触发的
    //当分区数为一时，不会调用RangePartitioner，则不会触发行动算子

    val partitioner: RangePartitioner[Int, Int] = new RangePartitioner(3, rdd1)

    val result: RDD[(Int, Int)] = rdd1.partitionBy(partitioner)


  }


  /*
      reduce
   */
  @Test
  def test3(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 1)

    println(rdd.reduce(_ + _))

  }

  /*
      collect
   */
  @Test
  def test4(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 1)

    //collect 将运算结果收集到driver端，在 driver端 遍历运算结果，打印
    //如果计算的结果数据量大，driver端就OOM
    rdd.collect().foreach(println)

  }

  /*
      count : 统计RDD中元素的个数
   */
  @Test
  def test5(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    println(rdd.count())

  }

  /*
      first: 取RDD中的第一个元素  没有对RDD重新排序，0号区的第一个
   */
  @Test
  def test6(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    println(rdd.first())

  }

  /*
  take： 取RDD的前n个元素，先从0号区取，不够再从1号区取
   */
  @Test
  def test7(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    println(rdd.take(3).mkString(","))

  }

  /*
      takeOrdered：先排序，再取前n
   */
  @Test
  def test8(): Unit = {
    val list = List(2,5,3,9,5,4,7)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    println(rdd.takeOrdered(6).mkString(","))


  }

  /*
      aggregate 和aggergateByKey类似，不要求数据是key-value，zeroValue既会在分区内使用，还会在分区间合并时使用
   */
  @Test
  def test9(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val result: String = rdd.aggregate("$")(_ + _, _ + _)

    println(result)

  }

  /*
      fold ：简化版aggregate。要求zeroValue必须和RDD中的元素类型一致，且分区内和分区间的运算逻辑一致
   */
  @Test
  def test10(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val result: Int = rdd.fold(11)( _ + _) // 11 + (11+1+2) + (11+3+4)

    println(result)
  }

  /*
      countByKey： 基于key的数量进行统计，要求RDD的元素是key-value类型
   */
  @Test
  def test11(): Unit = {
    val list = List(1, 2, 3, 4)
    val list1 = List((1, 1), (2, 1), (3, 2), (1, 1), (2, 1))

    val rdd1: RDD[Int] = sc.makeRDD(list, 3)
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(list1, 3)

    //rdd1.countByKey() //爆红，类型不为kv
    println(rdd2.countByKey())

  }

  /*
      countByValue:不要求RDD的元素时key-value类型，统计value出现的次数
   */
  @Test
  def test12(): Unit = {
    val list = List(1, 2, 3, 4)
    val list1 = List((1, 1), (2, 1), (3, 2), (1, 1), (3, 1))

    val rdd1: RDD[Int] = sc.makeRDD(list, 3)
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(list1, 3)

    println(rdd1.countByValue())
    println(rdd2.countByValue())
  }

  /*
      save相关
   */
  @Test
  def test13(): Unit = {

    val list = List(1, 2, 3, 4)
    val list1 = List((1, 1), (2, 1), (3, 2), (1, 1), (3, 1))

    val rdd1: RDD[Int] = sc.makeRDD(list, 3)
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(list1, 3)

    //保存为文本格式文件
    rdd1.saveAsTextFile("output")
    //保存为对象文件(将对象写入文件，对象要求序列化)
    rdd1.saveAsObjectFile("outputOB")

    //每个Record 都是key-value，多个Record组成一个block（16k）      *****************
    rdd2.saveAsSequenceFile("outputSq")



  }

  /*
     foreach
   */
  @Test
  def test14(): Unit = {

    val list = List(1, 2, 3, 4, 5)
    val rdd: RDD[Int] = sc.makeRDD(list, 3)

    // 在Driver端声明一个变量
    var sum = 0

    //遍历RDD中每个元素
    //foreach是算子，算子都是分布式运算，在Executor算
    //如果有闭包，闭包的变量，会被copy为副本，每个task都copy一份副本
    rdd.foreach(x => sum += x) //分布式运算

    //打印的是Driver端的sum
    println(sum) //0

  }

  /*
     foreach
   */
  @Test
  def test15(): Unit = {

    val list = List(1, 2, 3, 4, 5)
    val rdd: RDD[Int] = sc.makeRDD(list, 3)

    //在driver端
    rdd.collect().foreach(x =>{

      println(x + Thread.currentThread().getName)

    })

    println("-------------------")

    //在Executor
    rdd.foreach(x =>{

      //本地模式，只有一台worker，master也是自己
      println(x + Thread.currentThread().getName)  //并行

    })
  }

  /*
     foreach补充
   */
  @Test
  def test16 () : Unit = {
    val list = List(1, 2, 3, 4, 5)
    val rdd: RDD[Int] = sc.makeRDD(list, 3)

    //数据量大不建议
    rdd.collect()

    //让每个executor将结果写入到数据

    // Connertion conn = new Connection()
    rdd.foreach( x =>{
      //不用闭包
      // Connertion conn = new Connection()
      //conn.save(x)

    })

    //一个分区地创建一个连接
    rdd.foreachPartition(x =>{
      //一个分区创建一个匿名函数，Connection一个分区创建一个
      //Connertion conn = new Connection()
      //x.foreach( ele => xxx )
    })

    //异步，效率快
    //rdd.foreachPartitionAsync()

  }


}
