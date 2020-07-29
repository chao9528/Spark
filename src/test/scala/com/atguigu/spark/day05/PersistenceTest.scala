package com.atguigu.spark.day05

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before}
import org.junit._

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
   */
  @Test
  def test1 () : Unit = {
    val list = List(1, 2, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list, 2)


    val rdd2: RDD[Int] = rdd1.map(x => {
      println(x + "map")
      x
    })

    //将rdd2的运算结果缓存
    //val rdd: rdd2.type = rdd2.cache() //缓存是有缓存值
    //第一个行动算子执行时，将rdd2的结果缓存！ （懒加载）
    //默认将rdd2缓存到内存
    //rdd2.persist() //等价于rdd2.cache()

    //指定缓存的存储级别
    rdd2.persist(StorageLevel.MEMORY_AND_DISK) //存入内存和磁盘

    //Job1
    rdd2.collect()

    println("--------------------------")

    //Job2
    //从缓存中取出rdd2，写入文件
    rdd2.saveAsTextFile("output")

    Thread.sleep(100000000) //本地web：chao:4040查看job的DAG



  }

}
