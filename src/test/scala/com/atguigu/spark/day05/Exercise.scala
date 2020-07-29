package com.atguigu.spark.day05

/**
 *  Created by chao-pc  on 2020-07-15 10:17
 *      时间戳，省份，城市，用户，广告，中间字段使用空格分隔
 *    数据格式：1516609143867 6 7 64 16
 *
 *    统计出每一个省份 广告点击数量排行的Top3的广告
 *
 *        输入： 1516609143867 6 7 64 16   =>   ( (省份,广告), 1 )
 *
 *               ( (省份,广告), 1 ) =>   ( (省份,广告), N )
 *
 *               ( (省份,广告), N ) =>   ( 省份 ,(广告, N ) )
 *
 *                ( 省份 ,(广告, N ) ) =>  ( 省份 ,List ( (广告, N ) ，(广告, N )，(广告, N ) ...))
 *
 *                ( 省份 ,List ( (广告, N ) ，(广告, N )，(广告, N ) ...)) => 结果
 *
 *
 *        输出： List( (省份1,List(  ( 广告1, 点击量) , ( 广告1, 点击量), ( 广告1, 点击量) )  ) ,
 *               (省份2,List(  ( 广告1, 点击量) , ( 广告1, 点击量), ( 广告1, 点击量) )  ) )
 *
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.junit._

object Exercise {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //提前删除output fileSystem：本地文件系统
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    //声明结果输出路径
    val path: Path = new Path("output")
    //如果输出目录存在，就删除
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }


    val source: RDD[String] = sc.textFile("input/agent.log", 1)

    //1516609143867 6 7 64 16   =>   ( (省份,广告), 1 )
    val rdd1: RDD[((String, String), Int)] = source.map(line => {
      val words: Array[String] = line.split(" ")
      ((words(1), words(4)), 1)
    })
    rdd1.saveAsTextFile("output2")

    //( (省份,广告), 1 ) =>   ( (省份,广告), N )
    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)

    //( (省份,广告), N ) =>   ( 省份 ,(广告, N ) )
    val rdd3: RDD[(String, (String, Int))] = rdd2.map({
      case ((province, ads), count) => (province, (ads, count))
    })

    //( 省份 ,(广告, N ) ) =>  ( 省份 ,List ( (广告, N ) ，(广告, N )，(广告, N ) ...))
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    //( 省份 ,List ( (广告, N ) ，(广告, N )，(广告, N ) ...)) => 结果
    val result: RDD[(String, List[(String, Int)])] = rdd4.map({
      case (province, it) => (province, it.toList.sortBy(_._2)(Ordering[Int].reverse).take(3))
    })
    result.saveAsTextFile("output")

    sc.stop()
  }


}
