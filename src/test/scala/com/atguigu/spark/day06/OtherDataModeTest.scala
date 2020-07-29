package com.atguigu.spark.day06

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, _}

import scala.collection.mutable

/**
 *  Created by chao-pc  on 2020-07-17 10:16
 *
 *  累加器： 场景：计算，累加
 *          可以给累加器起名称，可以在WEB UI查看每个Task中累加器的值，以及汇总后的值！
 *          官方只提供可数值类型（Long，Double）的累加器，用户可以通过实现接口，自定义！
 *
 *          获取官方累加器： SparkContext.LongAccomulator
 *
 *          Task：调用add进行累加，不能读取值！
 *          Driver：调用value获取累加的值
 *
 *          自定义：实现AccumulatorV2
 *          必须实现的核心方法：①add : 累加器
 *                            ②reset : 重置累加器到0
 *                            ③merge : 合并其他累加器到一个累加器
 *
 *              创建： new
 *                    还需要调用 sparkContext.register()注册
 *             调用行动算子，才会实现真正的累加，不会重复累加，即便是task重启！
 *
 *             累加器在序列化到task之前： copy()返回当前累加器的副本 ---> reset()重置为0 ---->isZero(是否归0)
 *                          只有在isZero返回true，此时才会执行序列化！
 *                          false就报错！
 *
 *                      目的是为了保证累加器技术的精确性！
 *
 *              wordcount(单词,1)(单词,1) 可以使用累加器解决！
 *              好处：避免了shuffle，在Driver端聚合！
 *
 *              注意： 序列化到task之前，要返回一个归0累加器的拷贝！
 *
 *
 *  广播变量： 允许将广播变量发给每一个机器，而不是拷贝给每一个task！
 *
 *          使用场景：  需要在多个task中使用共同的大的只读的数据集！
 *
 *          作用：   节省网络传输消耗！
 *
 */


class OtherDataModeTest {
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


  /**
   * 引入案例
   */
  @Test
  def test1(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    var sum: Int = 0

    //构成闭包后，sum会以副本的形式复制发送到每一个task
    rdd.foreach(x => sum += x)

    //Driver中的sum : 0
    println(sum)

  }

  /**
   * 使用累加器获取累加的值
   */
  @Test
  def test2(): Unit = {
    val list = List(1, 2, 3, 4)

    //获取官方那个提供的累加器
    val acc: LongAccumulator = sc.longAccumulator("mysum")

    //在driver端先加10
    acc.add(10)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    //闭包，累加器以副本的形式发送到每个task
    rdd.foreach(x => acc.add(x))

    //Driver中获取累加器的值
    println(acc.value)

    Thread.sleep(1000000)
  }


  /**
   * 使用自定义累加器
   */
  @Test
  def test3(): Unit = {

    val list = List("hello", "hello", "hi", "hi", "hello")

    val rdd: RDD[String] = sc.makeRDD(list, 2)

    //创建累加器
    val acc = new WordCountAccumulator

    //注册
    sc.register(acc, "wordCount")

    //使用
    rdd.foreach(word => acc.add(word))

    //获取结果
    println(acc.value)


  }

  /**
   *广播变量的引入
   */
  @Test
  def test4(): Unit = {
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

    val list2 = List(4, 5, 6, 7, 8, 9, 10, 11) //假设很大

    val rdd: RDD[Int] = sc.makeRDD(list1, 4)

    //按照list2来过滤list1中的元素 list2是一个闭包变量，复制闭包变量的每一个副本到每一个task！
    rdd.filter(x => list2.contains(x))


  }

  /**
   *广播变量的使用
   */
  @Test
  def test6(): Unit = {
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

    val list2 = List(4, 5, 6, 7, 8, 9, 10, 11) //假设很大

    val list2Bc: Broadcast[List[Int]] = sc.broadcast(list2) //将list2广播


    val rdd: RDD[Int] = sc.makeRDD(list1, 4)

    //使用广播变量
    val rdd2: RDD[Int] = rdd.filter(x => list2Bc.value.contains(x))

    rdd2.collect().foreach(println)

  }

}


/**
 * 自定义累加器
 */
class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] { //输入字符串，输出可变的map集合

  //存放累加结果的集合
  private val wordMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

  //判断是否为空
  override def isZero: Boolean = wordMap.isEmpty

  //复制
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new WordCountAccumulator()

  //重置归0
  override def reset(): Unit = wordMap.clear()

  //累加
  override def add(v: String): Unit = {

    //累加单词时，更新wordMap
    wordMap.put(v, wordMap.getOrElse(v, 0) + 1)

  }

  //合并其他累加器的值到一个累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

    //两个累加器的wordMap合并到一个中
    //获取要合并的累加器中的Map
    val toMergeMap: mutable.Map[String, Int] = other.value

    for ((word, count) <- toMergeMap) {

      wordMap.put(word, wordMap.getOrElse(word, 0) + count)

    }

  }

  //获取累加后的值
  override def value: mutable.Map[String, Int] = wordMap

}
