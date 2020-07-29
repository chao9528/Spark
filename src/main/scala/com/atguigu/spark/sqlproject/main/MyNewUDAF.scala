package com.atguigu.spark.sqlproject.main

import java.text.DecimalFormat

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
 *  Created by chao-pc  on 2020-07-23 9:34
 *   MyUDAF(cityname) => 北京21.2%，天津13.2%，其他65.6%
 *
 *    作用: 统计每个地区,每个商品,不同的城市的点击次数
 *
 *              group by area,product_id
 *
 *          华北  商品A   北京
 *          华北  商品A   天津
 *          华北  商品A   北京
 *          华北  商品A   北京
 *
 *          MyUDAF (cityname)使用位置是,在 group by area,product_id 后进行统计
 *
 *          select MyUDAF(cityname)
 *          from xx
 *          group by area,product_id
 *
 *          输入: String : city
 *          输出: String : 计算此商品在此地区的点击总数
 *          缓冲(两个参数): Map[String,Long] 保存在每个城市的点击次数
 *                         Long: 计算此商品在此地区的点击总数
 *
 *          计算比例 = 城市的点击总数 / 地区的点击总数
 */
class MyNewUDAF extends Aggregator[String, MyBuffer, String] {

  //初始化缓冲区
  override def zero: MyBuffer = MyBuffer(Map[String, Long](), 0l)

  //分区内聚合 将a聚合到b上返回b
  override def reduce(b: MyBuffer, a: String): MyBuffer = {

    // input: Row 输入值 (城市名)
    // 为当前城市的点击数量+1  返回的是不可变的map
    val map: Map[String, Long] = b.map//接收缓冲的map

    val key: String = a //接受输入的城市字符串,用作map中的key值
    val value: Long = map.getOrElse(key, 0l) + 1l //设置缓冲中的value值,若有key,取value加一,若没有设置为0并加一

    b.map = map.updated(key, value) //更新map的值给缓冲区的零号索引处的map

    //为总数量+1
    b.sum = b.sum + 1
    
    b
  }

  //分区间聚合 将b2的值聚合到b1上
  override def merge(b1: MyBuffer, b2: MyBuffer): MyBuffer = {
    //取出map1和map2的索引0处的map值
    val map1: Map[String, Long] = b1.map
    val map2: Map[String, Long] = b2.map

    // 使用 foldLeft[B](z: B)(op: (B, A) => B) 从左向右
    val result: Map[String, Long] = map1.foldLeft(map2) { //向右合并缓冲区中map的value(count)值
      case (map, (city, count)) => {
        val sumCount: Long = map.getOrElse(city, 0l) + count
        map.updated(city, sumCount)
      }
    }
    //返回map结果
    b1.map = result

    //合并sum
    b1.sum = b1.sum + b2.sum

    b1
  }


  private val myFormat = new DecimalFormat("0.00%")
  //返回结果
  override def finish(reduction: MyBuffer): String = {
    //北京21.2%，天津13.2%，其他65.6%

    //2.对map进行排序取出前2 先将map转为List集合
    val top2: List[(String, Long)] = reduction.map.toList.sortBy(-_._2).take(2)

    //3.计算其他的数量
    val otherCount: Long = reduction.sum - top2(0)._2 - top2(1)._2

    //4.加入其它
    val result: List[(String, Long)] = top2 :+ ("其它", otherCount)

    //5.拼接字符串并返回
    val str: String = "" + result(0)._1 + " " + myFormat.format(result(0)._2 / reduction.sum.toDouble) +
      result(1)._1 + " " + myFormat.format(result(1)._2 / reduction.sum.toDouble) +
      result(2)._1 + " " + myFormat.format(result(2)._2 / reduction.sum.toDouble)
    str
  }

  //buffer的编码器
  override def bufferEncoder: Encoder[MyBuffer] = Encoders.product

  //输出的编码器 字符串
  override def outputEncoder: Encoder[String] = Encoders.STRING
}

case class MyBuffer(var map: Map[String, Long], var sum: Long)