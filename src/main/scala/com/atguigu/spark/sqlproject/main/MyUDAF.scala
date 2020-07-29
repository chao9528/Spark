package com.atguigu.spark.sqlproject.main

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

/**
 *  Created by chao-pc  on 2020-07-22 21:24
 *  MyUDAF(cityname) => 北京21.2%，天津13.2%，其他65.6%
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
 *
 */
class MyUDAF extends UserDefinedAggregateFunction {

  //输入的参数的结构  返回的是StructField类型的集合  输入城市的名字
  override def inputSchema: StructType = StructType(StructField("city", StringType) :: Nil)

  //缓冲区的结构   map用以保存每个城市的点击次数  Long 计算此商品在此地区的点击数
  override def bufferSchema: StructType = StructType(StructField("map", MapType(StringType, LongType)) ::
    StructField("sum", LongType) :: Nil)

  //返回值的类型 字符串类型,保存此商品在此地区的点击总数
  override def dataType: DataType = StringType

  //是否是确定性函数
  override def deterministic: Boolean = true

  // 初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer(0) = Map[String, Long]() //初始为map集合
    buffer(1) = 0l //初始值为0的long
  }

  // 分区内计算 将input的值,累加到buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // input: Row 输入值 (城市名)
    // 为当前城市的点击数量+1  返回的是不可变的map
    val map: collection.Map[String, Long] = buffer.getMap[String, Long](0) //接收缓冲的map

    val key: String = input.getString(0) //接受输入的城市字符串,用作map中的key值
    val value: Long = map.getOrElse(key, 0l) + 1l //设置缓冲中的value值,若有key,取value加一,若没有设置为0并加一

    buffer(0) = map.updated(key, value) //更新map的值给缓冲区的零号索引处的map

    //为总数量+1
    buffer(1) = buffer.getLong(1) + 1

  }

  //分区间计算 将buffer2的值,累加到buffer1
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    //取出map1和map2的索引0处的map值
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

    // 使用 foldLeft[B](z: B)(op: (B, A) => B) 从左向右
    val result: collection.Map[String, Long] = map1.foldLeft(map2) { //向右合并缓冲区中map的value(count)值
      case (map, (city, count)) => {
        val sumCount: Long = map.getOrElse(city, 0l) + count
        map.updated(city, sumCount)
      }
    }
    //返回map结果
    buffer1(0) = result

    //合并sum
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  private val myFormat = new DecimalFormat("0.00%")

  //返回最终结果  返回值为字符串
  override def evaluate(buffer: Row): Any = {

    //北京21.2%，天津13.2%，其他65.6%

    //1.取出缓冲结果中的map
    val resultMap: collection.Map[String, Long] = buffer.getMap[String, Long](0)

    //2.对map进行排序取出前2 先将map转为List集合
    val top2: List[(String, Long)] = resultMap.toList.sortBy(-_._2).take(2)

    //3.计算其他的数量
    val sum: Long = buffer.getLong(1)

    val otherCount: Long = sum - top2(0)._2 - top2(1)._2

    //4.加入其它
    val result: List[(String, Long)] = top2 :+ ("其它", otherCount)

    //5.拼接字符串并返回
    val str: String = "" + result(0)._1 + " " + myFormat.format(result(0)._2 / sum.toDouble) +
      result(1)._1 + " " + myFormat.format(result(1)._2 / sum.toDouble) +
      result(2)._1 + " " + myFormat.format(result(2)._2 / sum.toDouble)
    str

  }
}















