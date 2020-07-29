package com.atguigu.spark.project.acc

import com.atguigu.spark.project.bean.CategoryInfo
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/*
      select aa,bb,函数(cc),sum(xx)
      from  表名  join 表名  on  xx=xx
      where xxx
      group by xxx
      having

 */

/**
 *  Created by chao-pc  on 2020-07-18 10:17
 *  IN : 累加的类型
 *            选择切分后的字段作为累加数据：
 *                  acc.add(words(6) categoryId categoryId  )  =>   累加器中 多一个类别的点击信息    (categoryId ,1,0,0)
 *                  acc.add(words(8).split(",")(0) categoryId  )  =>   累加器中 多一个类别的下单信息    (categoryId ,0,1,0)
 *                  acc.add(words(10).split(",")(0) categoryId  )  =>   累加器中 多一个类别的支付信息    (categoryId ,0,0,1)
 *
 *             字符串： (String,String)  (类别,累加字段)
 *
 *                  仅仅有类别无法累加，不知道要累加当前类别的哪个属性！
 *                  随着类别再传入一个标识，标识用来表示累加的字段！
 *
 *                通过 "click" 标识是一次对点击的累加
 *                通过 "order" 标识是一次对下单的累加
 *                通过 "pay" 标识是一次对支付的累加
 *
 * OUT ： 最终输出的结果
 *          累加后的结果  Map{(类别1, (Int, Int, Int)) ，(类别2, (Int, Int, Int))，(类别3, (Int, Int, Int)) }
 *
 *            类型： Map[String, Tuple3]
 *                  Map[String, CategoryInfo]
 */
class Top10HotCategoryAcc extends AccumulatorV2[(String, String), mutable.Map[String, CategoryInfo]] {

  private val data: mutable.Map[String, CategoryInfo] = mutable.Map[String, CategoryInfo]()

  //是否归0
  override def isZero: Boolean = data.isEmpty

  //复制当前累加器
  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, CategoryInfo]] = new Top10HotCategoryAcc

  //归零
  override def reset(): Unit = data.clear

  //累加
  override def add(v: (String, String)): Unit = {

    val categoryName: String = v._1
    val field: String = v._2

    //将传入的数据，累加到data中

    //判断当前类型，是否已经在累加器中累加过，如果累加过，在已经累加过的CategoryInfo的基础上继续累加
    //如果当前累加类别不存在，创建一个空的CategoryInfo,提供累加
    val info: CategoryInfo = data.getOrElse(categoryName, CategoryInfo(categoryName, 0, 0, 0))

    field match {

      case "click" => info.clickCount += 1
      case "order" => info.orderCount += 1
      case "pay" => info.payCount += 1
      case _ =>

    }

    //将累加后的结果重新放入累加器
    data.put(categoryName, info)

  }

  //合并  两个累加器中的Map进行合并  将一个Map中的entry取出，放入另一个map，对同一个类型的CategoryInfo，进行属性的累加
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, CategoryInfo]]): Unit = {

    //获取要累加的数据
    val toMergeMap: mutable.Map[String, CategoryInfo] = other.value

    for ((categoryName, categoryBean) <- toMergeMap) {

      //根据categoryName,获取当前map中的CategoryInfo 的对象
      val info1: CategoryInfo = data.getOrElse(categoryName, CategoryInfo(categoryName, 0, 0, 0))

      info1.clickCount += categoryBean.clickCount
      info1.orderCount += categoryBean.orderCount
      info1.payCount += categoryBean.payCount

      //将累加后的结果重新放入累加器
      data.put(categoryName, info1)

    }

  }

  override def value: mutable.Map[String, CategoryInfo] = data
}
