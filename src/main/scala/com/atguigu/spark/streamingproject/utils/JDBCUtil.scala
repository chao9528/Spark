package com.atguigu.spark.streamingproject.utils

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {

    // 创建连接池对象
    var dataSource:DataSource = init()

    // 连接池的初始化
    def init():DataSource = {

        val paramMap = new java.util.HashMap[String, String]()
        paramMap.put("driverClassName", PropertiesUtil.getValue("jdbc.driver.name"))
        paramMap.put("url", PropertiesUtil.getValue("jdbc.url"))
        paramMap.put("username", PropertiesUtil.getValue("jdbc.user"))
        paramMap.put("password", PropertiesUtil.getValue("jdbc.password"))
        paramMap.put("maxActive", PropertiesUtil.getValue("jdbc.datasource.size"))

        // 使用Druid连接池对象
        DruidDataSourceFactory.createDataSource(paramMap)
    }

    // 从连接池中获取连接对象
    def getConnection(): Connection = {
        dataSource.getConnection
    }

    def main(args: Array[String]): Unit = {

        println(getConnection())

    }
}
