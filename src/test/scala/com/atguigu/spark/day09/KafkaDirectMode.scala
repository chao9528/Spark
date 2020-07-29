package com.atguigu.spark.day09

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit._

/**
 *  Created by chao-pc  on 2020-07-23 16:26
 *
 */
class KafkaDirectMode {

  /**
   * def createDirectStream[K, V](
   *       ssc: StreamingContext,       //上下文
   *       locationStrategy: LocationStrategy,   //为Exectuor中的消费者线程分配主题分区的负载均衡策略,通常使用这个 LocationStrategies.PreferConsistent
   *       consumerStrategy: ConsumerStrategy[K, V] //系统自动为消费者组分配分区!自动维护offset!  ConsumerStrategies.Subscribe
   *     ): InputDStream[ConsumerRecord[K, V]] = {
   *     val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
   *     createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc)
   *   }
   */
  @Test
  def test1 () : Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")//设置master
    val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

    /*
        ConsumerConfig

        earliest: 只在消费者组不存在时生效.当已经确定消费过之后,将不会从头开始执行
     */
    val kafkaParams = Map[String,String](
      "group.id"-> "atguigu", //用户组
      "bootstrap.servers"-> "hadoop102:9092", //启动服务器
      "client.id"-> "1",
      "auto.offset.reset"-> "earliest", //从头读取
      "auto.commit.interval.ms"->"500", //提交间隔
      "enable.auto.commit"->"true",//自动提交是否开启
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",//消费者需要指定kv的反序列化器
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //获取Reciever   Subscribe模式自动维护offset  消费
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("Hello"), kafkaParams)) //选择要订阅的topics

    //ds返回ConsumerRecord[String, String]  我们只处理其中的value
    val ds1: DStream[String] = ds.map(record => {

      /*if(record.value() == "d"){                 //开启此判断,消费d数据时,会丢数据
        throw new Exception("消费了d导致了异常")
      }*/

      record.value()
    })

    // 业务逻辑
    val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)


    //输出每个窗口计算后的结果的RDD的前100条数据!
    result.print(100)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()
  }

  /**
   *  解决丢数据:  ①取消自动提交
   *              ②在业务逻辑处理完成之后,再手动提交offset
   *
   *              spark允许设置checkpoint,在故障时,自动将故障的之前的状态(包含提交的offset)存储!
   *              可以在重启程序后,重建状态,继续处理!
   */
  @Test
  def test2 () : Unit = {
    def rebuild():StreamingContext ={ //设置checkpoint点的函数
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")//设置master
      val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

      //设置检查点存储的目录
      context.checkpoint("kafka")

      val kafkaParams = Map[String,String](
        "group.id"-> "atguigu", //用户组
        "bootstrap.servers"-> "hadoop102:9092", //启动服务器
        "client.id"-> "1",
        "auto.offset.reset"-> "earliest", //从头读取
        "auto.commit.interval.ms"->"500", //提交间隔
        "enable.auto.commit"->"false",//自动提交是否开启
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",//消费者需要指定kv的反序列化器
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )

      //获取Reciever   Subscribe模式自动维护offset  消费
      val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](context,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](List("Hello"), kafkaParams)) //选择要订阅的topics

      //ds返回ConsumerRecord[String, String]  我们只处理其中的value
      val ds1: DStream[String] = ds.map(record => {
        /*if(record.value() == "d"){
          throw new Exception("消费了d导致了异常")
        }*/
        record.value()
      })

      // 业务逻辑
      val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)


      //输出每个窗口计算后的结果的RDD的前100条数据!
      result.print(100)

      //返回context
      context
    }

    /*
        获取一个active的sc,或从checkpoint的目录中重建sc,或new
     */
    val context: StreamingContext = StreamingContext.getActiveOrCreate("Kafka", rebuild)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()

  }

}
