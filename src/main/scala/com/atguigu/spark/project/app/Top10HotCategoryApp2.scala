package com.atguigu.spark.project.app

import com.atguigu.spark.project.base.BaseApp
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 *  Created by chao-pc  on 2020-07-17 16:26
 *
 */
object Top10HotCategoryApp2 extends BaseApp {
  override val outPutPath: String = "output/top10hot2"

  def main(args: Array[String]): Unit = {

    runApp {

      //一次性封装所有的数据： 封装时，格式： （类别，clickcount，orderCount，payCount）  xxx union  all
      val sourceRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

      //封装好数据  先map在扁平化
      val dataToMerge: RDD[(String, (Int, Int, Int))] = sourceRdd.flatMap(line => {
        val words: Array[String] = line.split("_")

        if (words(6) != "-1") { //判断是否是点击数据

          //将单个元组包装为集合， 这是为了if条件判断中的格式统一
          List((words(6), (1, 0, 0)))

        } else if (words(8) != "null") { //判断是否是下单数据

          val catgorys: Array[String] = words(8).split(",")
          catgorys.map(catgory => (catgory, (0, 1, 0)))

        } else if (words(10) != "null") { //判断是否是支付数据

          val catgorys: Array[String] = words(10).split(",")
          catgorys.map(catgory => (catgory, (0, 0, 1)))

        } else { //啥也不是，返回空集合
          Nil
        }
      })

      //按照类别聚合bykey
      val mergeResult: RDD[(String, (Int, Int, Int))] = dataToMerge.reduceByKey({
        case ((clickCount1, orderCount1, payCount1), (clickCount2, orderCount2, payCount2)) =>
          (clickCount1 + clickCount2, orderCount1 + orderCount2, payCount1 + payCount2)
      })

      //对点击量，订单量，支付量 进行递减排序   并取出前10
      val finalResult: Array[(String, (Int, Int, Int))] = mergeResult.sortBy(x => x._2, numPartitions = 1)(
        Ordering.Tuple3(Ordering.Int.reverse, Ordering.Int.reverse, Ordering.Int.reverse),
        ClassTag(classOf[Tuple3[Int, Int, Int]])
      ).take(10)

      //写出
      sc.makeRDD(finalResult, 1).saveAsTextFile(outPutPath)


    }

  }

}
