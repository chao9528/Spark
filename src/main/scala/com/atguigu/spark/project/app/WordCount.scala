package com.atguigu.spark.project.app

import com.atguigu.spark.project.base.BaseApp

/**
 *  Created by chao-pc  on 2020-07-17 21:04
 */
object WordCount extends BaseApp {
  override val outPutPath: String = "output/wordcount" //重写抽象属性——输出的目录

  def main(args: Array[String]): Unit = {
    runApp {

      sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2).saveAsTextFile(outPutPath)

    }
  }

}
