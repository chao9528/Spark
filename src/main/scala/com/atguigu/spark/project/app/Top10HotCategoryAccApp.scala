package com.atguigu.spark.project.app

import com.atguigu.spark.project.acc.Top10HotCategoryAcc
import com.atguigu.spark.project.base.BaseApp
import com.atguigu.spark.project.bean.CategoryInfo
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 *  Created by chao-pc  on 2020-07-20 14:39
 *  使用累加器完成
 */
object Top10HotCategoryAccApp extends BaseApp {
  override val outPutPath: String = "output/top10hotAcc"

  def main(args: Array[String]): Unit = {

    runApp {

      val sourceRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

      //创建累加器
      val acc = new Top10HotCategoryAcc

      //注册
      sc.register(acc)

      //调用累加器
      sourceRdd.foreach(line => {

        val words: Array[String] = line.split("_") //将每行数据切开

        //判断是否是点击数据
        if (words(6) != "-1") {

          acc.add(words(6), "click")

        } else if (words(8) != "null") {

          val categorys: Array[String] = words(8).split(",")

          categorys.foreach(category => acc.add(category, "order"))

        } else if (words(10) != "null") {

          val categorys: Array[String] = words(10).split(",")

          categorys.foreach(category => acc.add(category, "pay"))

        } else {

        }

      })

      //获取到累加器的值
      val result: mutable.Map[String, CategoryInfo] = acc.value

      val list: List[CategoryInfo] = result.values.toList

      val finalResult: List[CategoryInfo] = list.sortBy(x =>
        (x.clickCount, x.orderCount, x.payCount)
      )(Ordering.Tuple3[Int, Int, Int](Ordering.Int.reverse, Ordering.Int.reverse, Ordering.Int.reverse)).take(10)

      sc.makeRDD(finalResult,1).saveAsTextFile(outPutPath)

    }

  }

}
