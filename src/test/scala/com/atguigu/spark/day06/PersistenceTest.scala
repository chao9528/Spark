package com.atguigu.spark.day06

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, _}

/**
 *  Created by chao-pc  on 2020-07-15 16:38
 *  持久化：将数据进行持久保存，一般持久化指将数据保存到磁盘！
 *
 */


class PersistenceTest {
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
      缓存：提高对重复对象查询的效率

            查询缓存的过程:  ①到缓存中查询，有就返回，没有就查数据库
                            ②缓存一般都使用内存
                            ③缓存都有回收策略
                            ④缓存有失效的情况(更新了缓存中的对象)

            缓存不会改变血缘关系！
   */
  @Test
  def test1(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list, 2)

    val rdd2: RDD[Int] = rdd1.map(x => {
      println(x + "map")
      x
    })


    println("--------------- 缓存之前的血缘关系 ------------")
    println(rdd2.toDebugString)

    //指定缓存的存储级别
    rdd2.persist(StorageLevel.MEMORY_AND_DISK) //存入内存和磁盘

    println("--------------- 缓存之后的血缘关系 ------------")
    println(rdd2.toDebugString)

    //Job1
    rdd2.collect()

    println("--------------------------")

    //Job2
    //从缓存中取出rdd2，写入文件
    rdd2.saveAsTextFile("output")

    Thread.sleep(100000000) //本地web：chao:4040查看job的DAG

  }


  /**
   * 如果再计算的阶段中产生shuffle，shuffle之后的数据会自动缓存！在执行时，发现重复执行shuffle之前的阶段，此时会跳过之前
   * 阶段的执行，直接从缓存中获取数据
   */
  @Test
  def test2(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list, 2)


    val rdd2: RDD[(Int, Int)] = rdd1.map(x => {
      println(x + "map")
      (x, 1)
    })

    //Job1
    val rdd3: RDD[(Int, Int)] = rdd2.reduceByKey(_ + _) //shuffle之后的数据会自动缓存

    //缓存
    rdd3.cache()   //关闭之后经过查看网页DAG发现还是使用到了缓存，因为shuffle之后的数据会自动缓存

    rdd3.collect()

    rdd3.saveAsTextFile("output")

    Thread.sleep(100000000) //本地web：chao:4040查看job的DAG


  }


  /**
   * checkpoint： 将部分阶段运行的结果，存储在持久化的磁盘中！
   *
   *      作用：将RDD保存到指定目录的文件中！
   *            切断当前RDD和父RDD的血缘关系
   *            在Job执行完成后才开始运行！
   *            强烈建议将checkpoint的RDD先缓存，不会造成重复计算！
   *
   *
   *      checkpoint也会提交一个Job，执行持久化！
   *      在第一个行动算子执行完成后，提交Job，运行！
   */
  @Test
  def test3 () : Unit = {
    val list = List(1, 2, 3, 4)

    //指定checkpoint的目录
    sc.setCheckpointDir("checkpoint")

    val rdd1: RDD[Int] = sc.makeRDD(list, 2)


    /*val rdd2: RDD[Int] = rdd1.map(x => {
      println(x + "map")
      x
    })

    val rdd3: RDD[Int] = rdd2.map(x => {
      println(x + "map")
      x
    })

    val rdd4: RDD[Int] = rdd3.map(x => {
      println(x + "map")
      x
    })

    val rdd5: RDD[Int] = rdd4.map(x => {
      println(x + "map")
      x
    })*/

    val rdd6: RDD[Int] = rdd1.map(x => {
      println(x + "map")
      x
    })


    println("--------------- 缓存之前的血缘关系 ------------")
    println(rdd6.toDebugString)

    //指定缓存的存储级别
    rdd6.checkpoint() //存入内存和磁盘
    //先缓存，checkpoint的Job就可以复用
    rdd6.cache()

    //Job1
    rdd6.collect()
    println("--------------- 缓存之后的血缘关系 ------------")
    println(rdd6.toDebugString)


    //Job2
    //从缓存中取出rdd2，写入文件
    rdd6.saveAsTextFile("output")
    Thread.sleep(100000000) //本地web：chao:4040查看job的DAG
  }
}
