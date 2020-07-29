package com.atguigu.spark.day10

/**
 *  Created by chao-pc  on 2020-07-24 9:06
 *
 *      DStream的转换分为无状态和有状态!
 *
 *            状态(state):是否可以保留之前批次处理的结果!
 *            无状态: 不会保留之前批次处理的结果!  每个批次都是独立的,是割裂的!
 *            有状态: 保留之前批次处理的结果! 新的批次在处理时,可以基于之前批次的结果,进行组合运算!
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.junit._

class DStreamTransformTest {

  /**
   *    transform: 将一个DStream,利用函数,将这个DStream中的所有的RDD,转换为其他的RDD,之后再返回新的DStream
   *
   *    将DStream的算子转换为RDD,调用RDD的算子进行计算!
   */
  @Test
  def test1(): Unit = {

    //设置master
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

    //获取数据对象 DStream 默认以\t为一行数据的分隔,每行数据为一条   创建输入流
    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    // 将之前的DStream[String] => tranform => DStream[(String,Int)]
    val dsStream: DStream[(String, Int)] = ds1.transform(rdd => {
      //单词统计
      val rdd2: RDD[(String, Int)] = rdd.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
      rdd2
    })

    //输出每个窗口计算后的结果的RDD的前100条数据!
    dsStream.print(100)
    //启动运算
    context.start()
    //阻塞当前,直到终止
    context.awaitTermination()
  }


  /**
   *  不用DStream的算子,而是转为DS或DF
   *      Dstream  =>  RDD  =>  DS/DF
   */
  @Test
  def test2(): Unit = {
    //设置master
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")

    val context = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import sparkSession.implicits._

    //获取数据对象DStream 默认以\t为一行数据的分隔,每行数据为一条
    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    // 用DF或DS处理数据
    val dsStream: DStream[Row] = ds1.transform(rdd => {
      val df: Dataset[String] = rdd.toDS()
      df.createOrReplaceTempView("a")
      val df1: DataFrame = sparkSession.sql("select * from a")
      df1.rdd
    })

    val result: DStream[String] = dsStream.map(row => row.getString(0))
    //输出每个窗口计算后的结果的RDD的前100条数据!
    result.print(100)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()
  }


  /**
   *  Join: 双流Join
   *            两种实时计算逻辑需求的场景!
   *
   *         流越多,需要的计算资源就越多!
   *
   *         采集周期必须是一致的!
   */
  @Test
  def test3(): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    // 一个应用里面只能有一个StreamingContext,一个应用中,采集周期必须是一致的!
    val context1 = new StreamingContext(conf, Seconds(5)) //使用StreamingContext的辅助器进行构造 ,每隔五秒作为一个批次进行处理
    //val context2 = new StreamingContext(conf, Seconds(3))

    //获取数据对象DStream 默认以\t为一行数据的分隔,每行数据为一条
    val ds1: ReceiverInputDStream[String] = context1.socketTextStream("hadoop102", 2222)
    val ds2: ReceiverInputDStream[String] = context1.socketTextStream("hadoop102", 3333)


    val ds3: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val ds4: DStream[(String, Int)] = ds2.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val ds5: DStream[(String, (Int, Int))] = ds3.join(ds4)
    //输出每个窗口计算后的结果的RDD的前100条数据!
    ds5.print(100)

    //启动运算
    context1.start()

    //阻塞当前,直到终止
    context1.awaitTermination()
  }

  /**
   *  统计从此刻开始,数据的累计结果!   将多个采集周期计算的结果进行累加运算!
   *    UpdateStateByKey
   *
   *    def updateStateByKey[S: ClassTag](    //数据是k-v类型
   *       updateFunc: (Seq[V], Option[S]) => Option[S]  //updateFunc会被当前采集周期中的每个key都进行调用,调用后将
   *                                                    当前key的若干value和之前采集周期,key最终的stage进行合并,合并后更新最新状态
   *
   *                                                    Seq[V]:当前采集周期中key对应的values
   *                                                    Option[S]:之前采集周期,key最新的stage
   *     ): DStream[(K, S)]
   *
   *    有状态的计算,由于需要checkpoint保存之前周期计算的状态,会造成出现大量的小文件
   *            解决:①自己通过程序将过期的小文件删除
   *                ②不使用checkpoint机制,而是自己持久化状态到数据库中
   */
  @Test
  def test4(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    val context = new StreamingContext(conf, Seconds(5))

    //为了保存之前批次运算的状态  设置存储路径
    context.checkpoint("ck411")

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    // updatereduceByKey 有状态计算
    val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).
      updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))

    //输出每个窗口计算后的结果的RDD的前100条数据!
    result.print(100)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()
  }

  /**
   * 统计1小时内,每间隔1分钟,所有的数据!
   *  def reduceByKeyAndWindow(      //有状态计算
   *       reduceFunc: (V, V) => V, //value之间的计算的函数
   *       windowDuration: Duration, //窗口的间隔
   *       slideDuration: Duration //滑动的步长周期
   *     ): DStream[(K, V)]
   *
   *     reduceByKeyAndWindow:在一个滑动的窗口内,计算一次
   *
   *        滑动的步长和窗口的范围必须是采集周期RDD的整数倍
   *
   *     reduceByKey : 一个采集周期(批次)计算一次
   *
   *     采集周期:  可以看作,计算的窗口大小和采集周期一致,且滑动的步长和采集周期一致!
   */
  @Test
  def test5 () : Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    val context = new StreamingContext(conf, Seconds(3))

    context.checkpoint("ck4111")

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).
      //reduceByKeyAndWindow(_+_,windowDuration=Seconds(6),slideDuration=Seconds(3))
      //reduceByKeyAndWindow(_+_,windowDuration=Seconds(6)) 若省略滑动步长,则默认使用采集周期作为滑动步长
      //如果窗口有重复计算,效率低,可以优化  invReduceFunc: (V, V) => V : 第一个v之前计算的结果,第二个v要离开窗口的value
      reduceByKeyAndWindow(_+_, _-_,windowDuration=Seconds(6),slideDuration=Seconds(3),
        filterFunc=_._2 != 0)

    //输出每个窗口计算后的结果的RDD的前100条数据!
    result.print(100)



    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()
  }

  @Test
  def test6 () : Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")

    val context = new StreamingContext(conf, Seconds(3))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    //先定义好窗口,之后的运算都在窗口中运算
    val result: DStream[(String, Int)] = ds1.window(Seconds(6)).
      flatMap(line => line.split(" ").map((_, 1))).reduceByKey(_ + _)

    result.print(100)

    //前缀和后缀是输出目录的前缀和后缀  前缀+周期的时间戳+后缀      实际生产中 一般将结果保存到数据库
    result.repartition(1).saveAsTextFiles("a","out")

    //foreachPartition  一个分区调用函数处理一次
    /*result.foreachRDD(rdd => rdd.foreachPartition(it =>{
      //新建数据库
      //准备sql
      //写出数据
    }))*/

    context.start() //启动运算

    context.awaitTermination() //阻塞当前线程,直到终止

  }


  /**
   * 如何优雅的关闭流式程序
   *      context.stop()
   */
  @Test
  def test7 () : Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    val context = new StreamingContext(conf, Seconds(3))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    val result: DStream[(String, Int)] = ds1.window(Seconds(6)).
      flatMap(line => line.split(" ").map((_, 1))).reduceByKey(_ + _)

    //输出每个窗口计算后的结果的RDD的前100条数据!
    result.print(100)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()

    //关闭 在一个新的线程中关闭,因为当前线程已经被阻塞了
    new Thread(){
      setDaemon(true) //设置为守护线程

      override def run(): Unit = {

        //在需要关闭的时候才关闭
        //用代码去判断当前是否需要关闭  true =程序代码
        while (!true){
          Thread.sleep(5000)
        }

        context.stop(true,true)//stopGracefully=true 优雅的关闭,等待最后一批的数据处理完成再关闭
      }

    }.start()

  }


  /**
   * DStream => transform =>  RDD/DF/DS
   *                          RDD.map
   *
   * DStream.map
   */
  @Test
  def test8 () : Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    val context = new StreamingContext(conf, Seconds(5))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    val ds2: DStream[String] = ds1.map(x => {
      println(Thread.currentThread().getName + ":ds1.map")
      x
    })

    //
    val ds3: DStream[String] = ds1.transform(rdd => {

      //随着采集周期,每批数据都会运行一次
      println(Thread.currentThread().getName + ":transform.map")
      val rdd1: RDD[String] = rdd.map(x => {
        println(Thread.currentThread().getName + ":rdd.map")
        x
      })
      rdd1
    })
    //输出每个窗口计算后的结果的RDD的前100条数据!
    //ds2.print(100)
    ds3.print(100)

    //启动运算
    context.start()

    //阻塞当前,直到终止
    context.awaitTermination()
  }


}





