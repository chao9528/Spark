package com.atguigu.spark.day03

/**
 *  Created by chao-pc  on 2020-07-13 9:17
 *    单值类型RDD操作
 * map
 * 小功能：从服务器日志数据apache.log中获取用户请求URL资源路径
 *
 * mapPartitions
 * 小功能：获取每个数据分区的最大值
 * map和mapPartitions的区别
 *
 * mapPartitionsWithIndex
 * 获取第二个数据分区的数据
 *
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

class SingleValueRDDTest {

  val conf = new SparkConf().setAppName("My app").setMaster("local[*]")
  val sc = new SparkContext(conf)

  //提供初始方法，完成输出目录的清理
  @Before
  def init(): Unit ={
    //提前删除output fileSystem：本地文件系统
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    //声明结果输出路径
    val path: Path = new Path("output")
    //如果输出目录存在，就删除
    if(fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }
  }

  //测试完成后关闭
  @After
  def stop()={
    sc.stop()
  }

  //测试封装
  @Test
  def test1(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    rdd.saveAsTextFile("output")

  }

  /*
  map
      map[U: ClassTag](f: T => U): RDD[U]
        对当前RDD中的每个元素执行map操作，返回一个新的元素，将元素放入新的MapPartitionsRDD中！

        特点：① map操作后，不会改变分区数
              ②分区之间的数据不会发生交换
   */
  @Test
  def test2 () : Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd1: RDD[Int] = rdd.map(_ + 1)

    rdd1.saveAsTextFile("output")

  }


  /*
      map特点： 分区之间是并行运算（真正并行取决于cores）
                同一个分区内，一个元素执行完所有操作之后，才执行下一个元素
   */
  @Test
  def test3 () : Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd1: RDD[Int] = rdd.map(x =>{
      println(x + "执行了第一次map操作！")
      x
    })
    val rdd2: RDD[Int] = rdd1.map(x =>{
      println(x + "执行了第二次map操作！")
      x
    })


    val rdd3: RDD[(Unit, Iterable[Int])] = rdd2.groupBy(x => {
      println(x + "执行了groupBy操作！")
    })


    rdd3.saveAsTextFile("output")
  }

  /*
      练习 :从服务器日志apache.log中获取用户请求URL资源路径
   */
  @Test
  def test4 () : Unit = {
    val rdd: RDD[String] = sc.textFile("input/apache.log",1)

    //分析日志数据，使用空格切分，取出第7个元素
    val result: RDD[String] = rdd.map(line => line.split(" ")(6))

    result.saveAsTextFile("output")

  }

  /*
      mapPartitions:  将一个分区作为一个整体，调用一次map函数，转换后生成新的分区集合！
      def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U]

        和map的区别：①传入的函数不同，map将一个元素转为另一个元素，mapPartitions将一个集合变为另一个集合！
                    ② mapPartitions逻辑： cleanedF(iter)    批处理
                      map逻辑：           iter.map(cleanF)   个体处理
                    ③ map是全量处理：RDD中有X个元素，返回的集合也有x个元素
                        mapPartitions只要返回一个集合，进行过滤或者添加操作！
                    ④本质是mapPartitions是一个集合调用一次
                        在特殊场景，节省性能，例如需要将一个分区的数据，写入数据库中
                    ⑤ map是将一个元素的所有转换操作运行结束后，再继续开始下一个元素！
                      mapPartitions：多个分区并行开始转换操作，一个分区的所有数据全部运行结束后，mapPartitions才结束！
                                    一旦某个分区中的元素没有处理完，整个分区的数据都无法释放！需要更大的内存！



      spark是分布式运行：  需要时刻分清  Driver 和 Executor
                        Executor执行的是Task(封装了RDD的执行逻辑)

   */
  @Test
  def test5 () : Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    // rdd.map : 调用的是spark的map
    // Interator.map: 调用的是scala集合中的map()
    //val result: RDD[Int] = rdd.mapPartitions(x => x.map(ele => ele * 2))

    //只返回奇数
    //val result: RDD[Int] = rdd.mapPartitions(x => x.filter(ele => ele%2 == 1))

    /* val rdd1: RDD[Int] = rdd.map(x => {
      Connection c= new Connection()//  使用map，在map中需要配置连接的话，因为是一个元素一个元素的执行，故有几个元素就创建几个，比较浪费性能，此时可使用mapPartitions
      c.insert(x)
      println(x+"执行了第一次Map操作！")
      x
    })*/

    //将分区中的奇数写入到数据库
    val result: RDD[Int] = rdd.mapPartitions(x => {
      //有几个分区就创建几次connection
      //val conf: Configuration = new Configuration()
      x.filter(ele => ele%2 == 1)
    })

    result.saveAsTextFile("output")

  }

  /*
      小功能：获取每个数据分区的最大值
   */
  @Test
  def test6 () : Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    //需要将一个分区的数据全部读入，取最大值
    val result: RDD[Int] = rdd.mapPartitions(it => List(it.max).iterator)

    result.saveAsTextFile("output")
  }

  /*
      mapPartitionsWithIndex  : 执行逻辑  f: (index, iter) :index是当前分区的索引
                                                            iter是分区的迭代器

                                将一个分区整体执行一次map操作，可以使用分区的index！
   */
  @Test
  def test7 () : Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val result: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => iter.map(elem => (index, elem)))

    result.saveAsTextFile("output")
  }

  /*
        获取第二个数据分区的数据
   */
  @Test
  def test8 () : Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val result: RDD[Int] = rdd.mapPartitionsWithIndex({

      case (index, iter) if (index == 1 ) => iter
      case _ => Nil.iterator

    })

    result.saveAsTextFile("output")

  }


}
