package com.atguigu.spark.project.app

import com.atguigu.spark.project.base.BaseApp

/**
 *  Created by chao-pc  on 2020-07-18 16:02
 *
 *  页面单跳转换流程统计： 一个页面的单向跳转 转换率统计
 *
 *      首页 ----> 搜索 ----> 点击 ----> 加入购物车 ----> 下单 ----> 支付
 *
 *      100w ----> 80w -----> 50w -----> 30w     ------> 5w   ----> 1w
 *
 *      单向跳转 :  直接跳转  ： 首页 ----->  搜索
 *                 间接跳转  ：  首页 ----->  点击
 *
 *       对象：  页面的单向跳转转换率
 *                A-->B 的单向跳转转换率 = 访问A-B页面的人数 / 访问A页面的人数
 *
 *
 */
object PageConversionApp extends BaseApp{
  override val outPutPath: String = "output/PageConversionApp"

  def main(args: Array[String]): Unit = {

    runApp{

    }

  }
}
