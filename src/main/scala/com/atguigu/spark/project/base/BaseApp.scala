package com.atguigu.spark.project.base

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  Created by chao-pc  on 2020-07-17 16:18
 *
 *    完成  创建环境，释放环境，删除输出目录
 */
abstract class BaseApp {

  //应用输出的目录
  val outPutPath : String

  //提供初始方法，完成输出目录的清理
  def init(): Unit = {
    //提前删除output fileSystem：本地文件系统
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    //声明结果输出路径
    val path: Path = new Path(outPutPath)
    //如果输出目录存在，就删除
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

  val conf = new SparkConf().setAppName("My app").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def runApp( op: =>Unit ): Unit ={

    //清理输出目录
    init()

    //核心运行子类提供的代码
    try {
      op
    } catch {
      case e: Exception => println("出现了异常:" + e.getMessage)
    } finally {
      //关闭连接
      sc.stop()
    }
  }

}
