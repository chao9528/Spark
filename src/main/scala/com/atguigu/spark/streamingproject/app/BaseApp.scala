package com.atguigu.spark.streamingproject.app

import com.atguigu.spark.streamingproject.utils.PropertiesUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 *  Created by chao-pc  on 2020-07-24 15:03
 */
class BaseApp {

  //提供一个context
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
  val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

  //消费者参数
  val kafkaParams = Map[String,String](
    "group.id"-> PropertiesUtil.getValue("kafka.group.id"), //用户组
    "bootstrap.servers"-> PropertiesUtil.getValue("kafka.broker.list"), //启动服务器
    "client.id"-> "1",
    "auto.offset.reset"-> "earliest", //从头读取
    "auto.commit.interval.ms"->"500", //提交间隔
    "enable.auto.commit"->"true",//自动提交是否开启
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",//消费者需要指定kv的反序列化器
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  //
  def getDataFromKafka()={
    //获取Reciever   Subscribe模式自动维护offset  消费
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("Hello"), kafkaParams)) //选择要订阅的topics
  }

  def getAllBeans()={

  }


}
