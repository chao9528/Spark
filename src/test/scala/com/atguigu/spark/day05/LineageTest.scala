package com.atguigu.spark.day05

/**
 *  Created by chao-pc  on 2020-07-15 16:11
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.junit._

class LineageTest {
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
        血缘关系：在stage执行时，让task根据血缘来计算
                  在容错时，根据血缘关系恢复数据！

        依赖： 主要是为了让DAG调度器，划分stage
              最开始只有一个窄依赖的stage
              每多一个宽依赖，就再生成一个stage

              stage数量 = 初始stage(1) + 宽依赖的数量
                当stage完成划分时，就会发生shuffle，影响性能！

              stage的数量看宽依赖的数量: 宽依赖数量 + 1
              task的数量：分区的数量
   */
  @Test
  def test1 () : Unit = {

    val list = List(1, 2, 3, 4)

    //ParallelCollectionRDD
    val rdd1: RDD[Int] = sc.makeRDD(list, 2)
    println(rdd1.dependencies) //List()  天生

    //MapPartitionsRDD
    val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
    println(rdd2.dependencies) //List(org.apache.spark.OneToOneDependency@5cb042da)  1对1的依赖  窄依赖

    //ShuffledRDD
    val rdd3: RDD[(Int, Iterable[Int])] = rdd2.groupByKey()

    // 获取血缘关系，为了方便根据血缘关系在task崩溃时，重建数据
    //println(rdd3.toDebugString)

    //依赖关系
    println(rdd3.dependencies) //List(org.apache.spark.ShuffleDependency@53b8afea)  Shuffle依赖 , 宽依赖

  }

  @Test
  def test2 () : Unit = {

    val list = List(1, 2, 3, 4)

    val rdd1: RDD[Int] = sc.makeRDD(list, 2)

    val rdd2: RDD[Int] = rdd1.map(x => x)

    val rdd3: RDD[(Int, Int)] = rdd2.map((_, 1))

    val rdd4: RDD[(Int, Iterable[Int])] = rdd3.groupByKey()

    rdd4.collect()

    rdd4.saveAsTextFile("output")

    Thread.sleep(100000000)

  }



}
