package com.atguigu.spark.project.app

import com.atguigu.spark.project.base.BaseApp
import org.apache.spark.rdd.RDD

/**
 *  Created by chao-pc  on 2020-07-17 16:26
 *
 *        逻辑分析：
 *              Hive表： Data(
 *date: String,//用户点击行为的日期
 *user_id: Long,//用户的ID
 *session_id: String,//Session的ID
 *page_id: Long,//某个页面的ID
 *action_time: String,//动作的时间点
 *search_keyword: String,//用户搜索的关键词
 *click_category_id: Long,//某一个商品品类的ID
 *click_product_id: Long,//某一个商品的ID
 *order_category_ids: String,//一次订单中所有品类的ID集合
 *order_product_ids: String,//一次订单中所有商品的ID集合
 *pay_category_ids: String,//一次支付中所有品类的ID集合
 *pay_product_ids: String,//一次支付中所有商品的ID集合
 *city_id: Long
 *)
 *
 *                        一条数据只会记录一种行为！
 *
 *                      SQL：
 *                            点击数：
 *
 *
 */
object Top10HotCategoryApp extends BaseApp {
  override val outPutPath: String = "output/top10hot"

  def main(args: Array[String]): Unit = {

    runApp {

      //统计点击数,下单数，支付数
      val datas: RDD[String] = sc.textFile("datas/user_visit_action.txt")


      println("----------点击数据------------")
      //过滤掉未点击的操作 where
      val filterData: RDD[String] = datas.filter(line => {
        val words: Array[String] = line.split("_")
        words(6) != "-1"
      })

      //在分组之前将数据变为对偶元组(点击操作,1)
      val mapData: RDD[(String, Int)] = filterData.map(line => {
        val words: Array[String] = line.split("_")
        (words(6), 1)
      })

      //对点击操作进行归约   (品类，点击数)
      val ClickResult: RDD[(String, Int)] = mapData.reduceByKey(_ + _)


      println("----------下单数据------------")
      //过滤未下单的操作 where
      val filterDataOrder: RDD[String] = datas.filter(line => {
        val words: Array[String] = line.split("_")
        words(8) != "null"
      })

      //将下单的每行数据转换为(品类，1)
      val orderDatas: RDD[(String, Int)] = filterDataOrder.flatMap(line => {

        val words: Array[String] = line.split("_")

        val catagerys: Array[String] = words(8).split(",")

        for (catagery <- catagerys) yield (catagery, 1)

      })

      //将下单的数据按照类别累加： (品类,1)...... => (品类,N)
      val orderResult: RDD[(String, Int)] = orderDatas.reduceByKey(_ + _)


      println("----------支付数据------------")
      //过滤未支付的操作 where
      val filterDataPay: RDD[String] = datas.filter(line => {
        val words: Array[String] = line.split("_")
        words(10) != "null"
      })

      //将支付的每行数据转换为(品类，1)
      val payDatas: RDD[(String, Int)] = filterDataPay.flatMap(line => {

        val words: Array[String] = line.split("_")

        val catagerys: Array[String] = words(10).split(",")

        for (catagery <- catagerys) yield (catagery, 1)

      })

      //将支付的数据按照类别累加： (品类,1)...... => (品类,N)
      val payResult: RDD[(String, Int)] = payDatas.reduceByKey(_ + _)


      //合并支付数据 和 下单数据 和 点击数据为 (品类 , 点击, 下单, 支付)
      val joinDatas: RDD[(String, ((Int, Option[Int]), Option[Int]))] = ClickResult.leftOuterJoin(orderResult).leftOuterJoin(payResult)

      //使用map转换输出格式 下单和支付若为空则设置为0
      val conversionJoinDatas: RDD[(String, (Int, Int, Int))] = joinDatas.map {
        case (catagery, ((clickCount, orderCount), payCount)) => (catagery, (clickCount, orderCount.getOrElse(0), payCount.getOrElse(0)))
      }

      //将结果收集起来转换为List方便排序
      val result: List[(String, (Int, Int, Int))] = conversionJoinDatas.collect().toList

      //对数组中的每个对偶元组的第二个数据（三元组）进行排序，使用默认提供的Int的降序排
      val top10Caterogy: List[(String, (Int, Int, Int))] = result.sortBy(x => x._2)(
        Ordering.Tuple3(Ordering.Int.reverse, Ordering.Int.reverse, Ordering.Int.reverse)).take(10)

      sc.makeRDD(top10Caterogy,1).saveAsTextFile(outPutPath)


    }

  }

}
