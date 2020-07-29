package com.atguigu.spark.day09

/**
 *  Created by chao-pc  on 2020-07-23 10:23
 *
 *      离线(不在线)数据: 不是此时此刻正在生成的数据! 明确的生命周期!
 *      实时数据: 此时此刻正在生成的数据! 只有开始没有结束! 源源不断的流式数据!
 *
 *      离线计算: 计算需要花费一定的周期,才能生成结果! 不能立刻出结果!
 *      实时计算: 立刻出结果!
 *
 *      Spark Streaming: 准实时! 接近实时!  微批次处理!  本质上还是批处理! 每次处理的一批数据量不是很大!
 *              流式处理!  数据是不是源源不断!
 *
 *      基本的数据抽象:  定义数据源,定义数据的处理逻辑
 *          SparkCore : RDD
 *          SparkSQl : DataFrame,DataSet
 *                      就是对RDD的封装
 *          SparkStreaming : DStream(离散化流),流式数据可以离散分布到多个Excutor进行并行计算!
 *                DStream就是对RDD在实时数据处理场景的一种封装
 *
 *      数据的语义:
 *          at most once  : 至多一次   0次或1次 , 有可能会丢数据!
 *          at least once : 至少一次  不会丢数据!  有可能会重复!
 *          exactly once  : 精准一次
 *
 *
 *      整体架构: ①SparkStreaming一开始运行,就需要有Driver,还必须申请一个Executor,运行一个不会停止的task!
 *                    这个task负责运行reciever,不断接受数据
 *
 *               ②满足一个批次数据后,向Driver汇报,生成Job,提交运行Job,由receiver将这批数据的副本发送到Job运行的Excutor
 *
 *
 *      有StreamingContext作为应用程序上下文:
 *                只能使用辅助构造器构造!
 *                可以基于master的 url和appname构建,或基于sparkconf构建,或基于一个SparkContext构建!
 *
 *                获取StreamingContext中关联的SparkContext:StreamingContext.SparkContext
 *
 *                得到或转换了Dstream后，Dstream的计算逻辑，会在StreamingContext.start()后执行，在StreamingContext.stop()后结束！
 *
 *                StreamingContext.awaitTermination()： 阻塞当前线程，直到出现了异常或StreamingContext.stop()！
 *
 *                作用: 创建Dstream(最基本的数据抽象模型)
 */

/**StreamingContext 源码解释
 * Main entry point for Spark Streaming functionality. It provides methods used to create
 * [[org.apache.spark.streaming.dstream.DStream]]s from various input sources.
 *
 * It can be either created by providing a Spark master URL and an appName, or from a org.apache.spark.SparkConf
 * configuration (see core Spark documentation), or from an existing org.apache.spark.SparkContext.
 *
 * The associated SparkContext can be accessed using `context.sparkContext`.
 * After creating and transforming DStreams, the streaming computation can be started and stopped
 * using `context.start()` and `context.stop()`, respectively.
 * `context.awaitTermination()` allows the current thread to wait for the termination
 * of the context by `stop()` or by an exception.
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit._

import scala.collection.mutable

class HelloWorldTest {

  /**
   * 使用netcat 绑定一个指定的端口,让SparkStreaming程序,读取端口指定的输出信息!
   *
   */
  @Test
  def test1 () : Unit = {
    //设置master
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")

    val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

    //获取数据对象DStream 默认以\t为一行数据的分隔,每行数据为一条
    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    //执行各种转换 单词统计
    val ds2: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //输出每个窗口计算后的结果的RDD的前100条数据!
    ds2.print(100)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()
  }

  /**
   * 创建一个Quene[RDD], 让接收器每次接受队列中的一个RDD进行运算!
   *  def queueStream[T: ClassTag](
   *       queue: Queue[RDD[T]], //
   *       oneAtATime: Boolean,
   *       defaultRDD: RDD[T]
   *     ): InputDStream[T] = {
   *     new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
   *   }
   *
   */
  @Test
  def test2 () : Unit = {

    //设置master
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")

    val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

    //创建一个可变的Queue
    val queue: mutable.Queue[RDD[String]] = mutable.Queue[RDD[String]]()

    val ds: InputDStream[String] = context.queueStream(queue, false, null)

    val result: DStream[(String, Int)] = ds.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print(100)

    //启动运算
    context.start()

    val rdd: RDD[String] = context.sparkContext.makeRDD(List("hello James hello kobe"), 1)

    //向Queue中放入RDD
    for ( i<-1 to 100 ){
      Thread.sleep(1000)
      queue.enqueue(rdd)
    }


    //阻塞当前,直到终止
    context.awaitTermination()
  }







}













