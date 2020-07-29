package com.atguigu.spark.day04

/**
 *  Created by chao-pc  on 2020-07-14 9:23
 *
 *      双value，两个单value的RDD。理解为两个RDD集合进行转换！
 *
 * intersection： 交集
 * 如果两个RDD数据类型不一致怎么办？
 *
 *
 * union： 并集
 * 如果两个RDD数据类型不一致怎么办？
 *
 * subtract： 差集
 * 如果两个RDD数据类型不一致怎么办？
 *
 * cartesian
 *
 * zip
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

class DoubleValueRDDTest1 {
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


  /*
      defaultPartitioner(self, other) :返回一个分区器！ 将当前的RDD和要进行操作的N个RDD一起传入，获取这些操作RDD之后，要使用的分区器

                        numPartitions：如果设置了spark.default.parallelism，就使用它作为总的分区数！
                                       如果没有设置spark.default.parallelism，就使用上游的最大值！

                        分区逻辑：默认使用上游numPartitions最大的上游RDD的分区逻辑，如果不可用，则使用HashPartitioner
                                  的分区策略


      abstract class Partitioner extends Serializable {
       def numPartitions: Int //当前分区器共有多少个分区
       def getPartition(key: Any): Int  //计算k所属的分区 即分区逻辑
      }
   */
  /*
      intersection:交集，会造成shuffle！
                    最终会以上游RDD中分区数大的RDD的分区数作为最终的结果输出的分区数！

       只要是Key-Value类型，默认就使用HashPartitioner分区！
               输出结果：分区总数：上游RDD中分区数最大的RDD的分区数
                         怎么分：HashPartitioner对key做分区！

      如果两个RDD数据类型不一致怎么办?
          有泛型约束！ 两个集合的泛型必须一致！

          如果可以运行，做交集运算时，要求类型和值必须一致！

   */
  @Test
  def test1(): Unit = {

    val list1 = List(1, 2, 3, 4)
    val list2 = List(5, 6, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list1, 2)
    val rdd2: RDD[Int] = sc.makeRDD(list2, 4)

    rdd1.intersection(rdd2).saveAsTextFile("output")
  }


  /*
      List(1,2,3)   List(1,2,3)
          数据操作： union : (1,2,3,1,2,3)
          数学集合： union : (1,2,3)
       union:   并集
              将所有RDD的分区汇总！  不会有shuffle！ 除非，union.distinct

      如果两个RDD数据类型不一致怎么办?
          有泛型约束！
   */
  @Test
  def test2(): Unit = {

    val list1 = List(1, 2, 3, 4)
    val list2 = List(5, 6, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list1, 2)
    val rdd2: RDD[Int] = sc.makeRDD(list2, 4)

    //union之前先将两个RDD的所有分区合并汇总
    rdd1.union(rdd2).distinct().saveAsTextFile("output")

  }


  /*
      subtract:差集，以当前 this RDD的分区器和分区大小为准!
            this.subtract(other)

      有shuffle
   */
  @Test
  def test3 () : Unit = {
    val list1 = List(1, 2, 3, 4.0)
    val list2 = List(5, 6, 3, 4)

    val rdd1: RDD[AnyVal] = sc.makeRDD(list1, 3)
    val rdd2: RDD[AnyVal] = sc.makeRDD(list2, 4)

    rdd1.subtract(rdd2).saveAsTextFile("output")
  }


  /*
    cartesian :笛卡尔积
           返回的是数据(a,b),返回的RDD的分区个数为两个RDD分区的乘积！

           分区逻辑是ParallelCollectionRDD分区逻辑！
   */
  @Test
  def test4 () : Unit = {
    val list1 = List(1, 2, 3, 4.0)
    val list2 = List(5, 6, 3, 4)

    val rdd1: RDD[AnyVal] = sc.makeRDD(list1, 3)
    val rdd2: RDD[AnyVal] = sc.makeRDD(list2, 4)

    rdd1.cartesian(rdd2).saveAsTextFile("output")

  }


  /*
      zip：拉链  和scala的zip一样，返回两个RDD相同位置的元素组成的kay-value对
              假设两个RDD中的分区数一致，且分区


      如果两个RDD数据类型不一致怎么办？  可以运行，没有泛型约束！
      如果两个RDD数据分区不一致怎么办？  报错！要求分区数量一致
      如果两个RDD分区数据数量不一致怎么办？  报错！要求数据元素数量一致

          无shuffle！

      zipWithIndex
      ZipPartitions
   */
  @Test
  def test5 () : Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(5, 6, 3, 4)

    val rdd1: RDD[AnyVal] = sc.makeRDD(list1, 3)
    val rdd2: RDD[AnyVal] = sc.makeRDD(list2, 3)

    rdd1.zip(rdd2).saveAsTextFile("output")
  }

  /*
  zipWithIndex
   */
  @Test
  def test6 () : Unit = {
    val list1 = List(1, 2, 3, 4)

    val rdd1: RDD[AnyVal] = sc.makeRDD(list1, 3)

    //当前rdd的每个元素和元素的索引拉链
    rdd1.zipWithIndex().saveAsTextFile("output")

  }

  /*
  ZipPartitions:将当前RDD的T类型和其他RDD的U类型，通过函数进行操作，之后返回任意类型！
   */
  @Test
  def test7 () : Unit = {
    val list1 = List(1, 2, 3, 4,9)
    val list2 = List(5, 6, 3, 4)

    val rdd1: RDD[AnyVal] = sc.makeRDD(list1, 3)
    val rdd2: RDD[AnyVal] = sc.makeRDD(list2, 3)

    //zipAll是scala中提供的
    rdd1.zipPartitions(rdd2)( (it1,it2) => it1.zipAll(it2,10,20) ).saveAsTextFile("output")
  }

}
