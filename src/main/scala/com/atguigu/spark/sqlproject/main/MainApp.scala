package com.atguigu.spark.sqlproject.main

import org.apache.spark.sql.{SparkSession, functions}

/**
 *  Created by chao-pc  on 2020-07-22 17:10
 */
object MainApp {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("df")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .getOrCreate()


    val sql1: String = //使用城市id连接三个表
      """
        |
        |select
        |   ci.*,
        |   p.product_name,
        |   uv.click_product_id
        |from  user_visit_action uv
        |join  city_info ci on uv.city_id=ci.city_id
        |join  product_info p on uv.click_product_id=p.product_id
        |
        |""".stripMargin

    //注册
    // 2.0 API
    //val myUDAF = new MyUDAF

    val myNewUDAF = new MyNewUDAF

    // 注册2.0
    // sparkSession.udf.register("myudaf",myUDAF)
    // 注册新api
    sparkSession.udf.register("myudaf", functions.udaf(myNewUDAF))


    val sql2: String = //使用区域,点击数,和商品id对表进行分组,并统计个数
      """
        |select
        |     area,product_name,count(*) clickCount , myudaf(city_name) result
        |from t1
        |group by area,click_product_id,product_name
        |
        |""".stripMargin

    val sql3: String = //使用区域对表进行分区,每个区中按照统计的点击量降序排列
      """
        |
        |select  area,product_name,clickCount,result, rank() over(partition by area order by clickCount desc) rn
        |from t2
        |
        |""".stripMargin

    val sql4: String = //取出表中每个区域的前三
      """
        |select
        |    area,product_name,clickCount ,result
        |from t3
        |where rn <=3
        |""".stripMargin

    sparkSession.sql(sql1).createTempView("t1")
    sparkSession.sql(sql2).createTempView("t2")
    sparkSession.sql(sql3).createTempView("t3")
    sparkSession.sql(sql4).show(false)

    sparkSession.stop()


  }

}
