package com.atguigu.spark.day04

/**
 *  Created by chao-pc  on 2020-07-14 11:11
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}
import org.junit._

class KeyValueRDDTest1 {
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


  @Test
  def test1(): Unit = {
    /*val list1 = List(1, 2, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list1, 2)

    rdd1.partition*/
    //只有key-value类型的RDD可以调用

    /*
          在 object RDD 中：
           implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
            new PairRDDFunctions(rdd)
            }

        传入一个RDD[(K, V)] 自动转为   PairRDDFunctions
        调用 PairRDDFunctions提供的方法，例如partitionBy()
        有隐式转换：  ParallelCollectionRDD  转为  PairRDDFunctions
     */
    /*
           返回由传入的分区器，分区后的新的RDD
           分区器，系统已经提供了默认实现：
              HashPartitioner：
                    分区数由主构造传入！
                    分区计算，采取key的hashcode % 总的分区数
                          如果为正数，就直接返回
                          如果为负数，控制分区号在  [0,总的分区数)

                     比较两个HashPartitioner的对象时，只比较两个HashPartitioner的总的分区数是否相等，只要相等，就视为两个对象相等！


              RangePartitioner:  按照范围，对RDD中的数据进行分区！ 尽量保证每个分区的数据量是大约相等的！

                  def numPartitions: Int = rangeBounds.length + 1

                  rangeBounds： 范围边界！

                  传入的分区数和最后生成的总的分区数无直接关系！

                  局限性： 只能对Key能排序的数据做分区！

         */
    val list = List((1, 1.1), (2, 1.2), (1, 1.2), (2, 1.1))

    val rdd: RDD[(Int, Double)] = sc.makeRDD(list, 2)

    /*
        返回由传入的分区器，分区后的新的RDD
        只有key-value类型的RDD可以调用
     */
    val rdd1: RDD[(Int, Double)] = rdd.partitionBy(new HashPartitioner(4))

    rdd1.saveAsTextFile("output")

    //从源码中得出 ，再次传入相同的分区逻辑，不会再进行shuffle，默认使用上次的分区逻辑，    避免了一次shuffle！返回rdd2
    rdd1.partitionBy(new HashPartitioner(4)).saveAsTextFile("output1")


  }


  /*
              RangePartitioner:  按照范围，对RDD中的数据进行分区！ 尽量保证每个分区的数据量是大约相等的！

                  def numPartitions: Int = rangeBounds.length + 1

                  rangeBounds： 范围边界！

                  传入的分区数和最后生成的总的分区数无直接关系！

                  局限性： 只能对Key能排序的数据做分区！
   */
  @Test
  def test2(): Unit = {

    val list = List((1, 1.1), (2, 1.2), (1, 1.2), (2, 1.1), (1, 1.1), (3, 1.2), (4, 1.2), (5, 1.1))
    val rdd: RDD[(Int, Double)] = sc.makeRDD(list, 2)
    rdd.partitionBy(new RangePartitioner(3, rdd)).saveAsTextFile("output")

  }


  @Test
  def test3(): Unit = {
    val list = List(Person1("James", 35), Person1("Curry", 13), Person1("Harden", 30), Person1("jake", 35), Person1("kobe", 13), Person1("lin", 30))

    val rdd: RDD[Person1] = sc.makeRDD(list, 2)

    val rdd2: RDD[(Person1, Int)] = rdd.map(x => (x, 1))

    rdd2.partitionBy(new MyPartitioner(3)).saveAsTextFile("output")

  }

  /*
      mapValues
   */
  @Test
  def test4(): Unit = {

    val list = List(("a", 1), ("b", 1), ("c", 1), ("d", 1))

    val rdd: RDD[(String, Int)] = sc.makeRDD(list)

    val rdd1: RDD[(String, Int)] = rdd.mapValues(value => value * 2)

    println(rdd1.collect().mkString(","))

  }


  /*
      reduceBykey:对每一个key的values进行reduce操作，在将reduce的结果发给reducer之前，
                  会在mapper的本地执行reduce，类似MR中的combiner

                  不能改变RDD中value对应的类型！

          存在shuffle！ 将计算后的结果，通过 当前RDD的分区器，分当前RDD指定的分区数量！

      reduceBykey和groupByKey的区别？
              功能上： 一个是分组，一个是分组后进行reduce运算！
              性能上：  都有shuffle。
                         reduceByKey可以在本地聚合，shuffle的数据量小！
                         groupByKey没有本地聚合，shuffle的数据量大！

   */
  @Test
  def test5(): Unit = {

    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    val result: RDD[(Int, Int)] = rdd.reduceByKey(_ + _)

    result.saveAsTextFile("output")

    //在本地spark中测试性能：

    sc.makeRDD(list, 2).reduceByKey(_ + _).collect() //100 B

    sc.makeRDD(list, 2).groupByKey.collect() //116 B

  }


  /*
      aggregateByKey:先传入一个数，作为分区内操作的第一个操作数，再分别传入分区内和分区间的操作函数

      aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)]

      ①对每个key的value进行聚合
      ②saqOp函数通过 zeroValue U 和value V 进行运算，返回 U 类型数据
          在每一个分区内部运算
      ③combOp：分区间，再对相同key计算的结果进行合并
      ④zeroValue：只在分区内运算时使用！

      取出每个分区内相同key的最大值然后分区间相加
      分区内同时求最大和最小，分区间合并
      求每个key对应的平均值

   */
  @Test
  def test6(): Unit = {
    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    //默认使用0作为分区内相加操作的第一个操作数
    //rdd.aggregateByKey(0)( _ + _, _ + _ ).saveAsTextFile("output")

    //使用字符"_"作为第一个操作数，故分区内和分区间的相加操作均为字符串的拼接
    rdd.aggregateByKey("_")(_ + _, _ + _).saveAsTextFile("output")

  }


  /*
      取出每个分区内相同key的最大值然后分区间相加
   */
  @Test
  def test7(): Unit = {
    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    rdd.aggregateByKey(Int.MinValue)((zero, value) => zero.max(value),
      (max1, max2) => max1 + max2).saveAsTextFile("output")

  }


  /*
      分区内同时求最大和最小，分区间合并
   */
  @Test
  def test8(): Unit = {

    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    val result: RDD[(Int, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
      { //模式匹配
        case ((min, max), value) => (min.max(value), max.min(value)) //区间内操作 每个value都与zoreValue中的min和max进行操作
      }, {
        case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
      }
    )

    result.saveAsTextFile("output")

  }


  /*
      求每个key的平均值
            结果： (key,avg) =(key,sum/count)
   */
  @Test
  def test9(): Unit = {
    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    val rdd1: RDD[(Int, (Int, Int))] = rdd.aggregateByKey((0, 0))( //sum和count初始默认为 0
      {
        case ((sum, count), value) => (sum + value, count + 1) //sum累加，并且每次次数加一
      }, {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) //分区间合并
      }
    )

    //对rdd1中的value进行操作
    val result: RDD[(Int, Double)] = rdd1.mapValues({
      case (sum,count) =>sum.toDouble/count
    })

    result.saveAsTextFile("output")


  }


  /*
      foldByKey: aggregateByKey的seqOp和conbOp一样时，且aggregateByKey的zeroValue类型和value的类型一致时
                等价于 foldByKey
                eg:分区内相加和分区间相加如下
   */
  @Test
  def test10(): Unit = {
    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    rdd.aggregateByKey(0)(_+_,_+_).saveAsTextFile("output")
    rdd.foldByKey(0)(_+_).saveAsTextFile("output1")//等价与上述aggregateByKey的写法

  }


  /*
      combineByKey
      combineByKey[C](     //需要指定每个操作数的数据类型
      createCombiner: V => C,  //制造和value不同的zero。
                                  使用集合中的一个元素，通过createCombiner函数，计算得到zeroValue
      mergeValue: (C, V) => C, //分区内对value进行聚合
      mergeCombiners: (C, C) => C,//分区间对value进行聚合
      partitioner: Partitioner,

      mapSideCombine: Boolean = true,
      serializer: Serializer = null): RDD[(K, C)]
   */
  @Test
  def test11(): Unit = {

    val list = List((1, 1), (2, 1), (3, 1), (4, 1), (1, 1), (2, 1), (3, 1), (4, 1),
      (1, 2), (2, 2), (3, 2), (4, 2), (1, 2), (2, 2), (3, 2), (4, 2))

    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    rdd.aggregateByKey(10)(_+_,_+_).saveAsTextFile("output")

    val result: RDD[(Int, Int)] = rdd.combineByKey(  //等价于上述aggregateByKey的写法
      (value: Int) => 10, //分区间操作的第一个操作数的初始值
      (x: Int, y: Int) => x + y,//分区内操作
      (x: Int, y: Int) => x + y,//分区间操作
      new HashPartitioner(2)
    )

    result.saveAsTextFile("output1")

  }


}


class MyPartitioner(nums: Int) extends Partitioner { //定义我的分区类继承Partitioner重写其中的方法
  override def numPartitions: Int = nums

  // 对Person的age进行分区
  override def getPartition(key: Any): Int = {
    if (!key.isInstanceOf[Person1]) {
      0
    } else {
      val person: Person1 = key.asInstanceOf[Person1]
      person.age.hashCode() % numPartitions
    }
  }
}

/*
从源码的角度来讲，四个算子的底层逻辑是相同的。 最终都调用了combineByKeyWithClassTag ，运算过程中都会有shuffle
ReduceByKey不会对第一个value进行处理，分区内和分区间计算规则相同。
AggregateByKey的算子会将初始值和第一个value使用分区内计算规则进行计算。
FoldByKey的算子的分区内和分区间的计算规则相同，初始值和第一个value使用分区内计算规则。
CombineByKey的第一个参数就是对第一个value进行处理，所有无需初始值
*/























