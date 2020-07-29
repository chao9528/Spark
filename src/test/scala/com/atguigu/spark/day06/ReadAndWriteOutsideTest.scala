package com.atguigu.spark.day06

/**
 *  Created by chao-pc  on 2020-07-17 11:32
 */

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, _}

class ReadAndWriteOutsideTest {
  val conf = new SparkConf().setAppName("My app").setMaster("local[*]")
  val sc = new SparkContext(conf)

  //提供初始方法，完成输出目录的清理
  @Before
  def init(): Unit = {
    //提前删除output fileSystem：本地文件系统
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    //声明结果输出路径
    val path: Path = new Path("output")
    //如果输出目录存在，就删除
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

  //测试完成后关闭
  @After
  def stop() = {
    sc.stop()
  }


  /**
   * 读写文本文件
   */
  @Test
  def test1(): Unit = {

    // new HadoopRDD 从文本文件中读
    val rdd: RDD[String] = sc.textFile("input")

    rdd.saveAsTextFile("output")

  }

  /**
   * 写对象文件
   */
  @Test
  def test2(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 1)

    //写如对象文件obj中
    rdd.saveAsObjectFile("obj")

  }

  /**
   * 读对象文件
   */
  @Test
  def test3(): Unit = {

    //从对象文件obj中读取文件
    val rdd: RDD[Int] = sc.objectFile[Int]("obj")

    rdd.foreach(println)

  }

  /**
   * 写SequenceFile
   */
  @Test
  def test4 () : Unit = {

    val list = List(1, 2, 3, 4)

    val add: RDD[Int] = sc.makeRDD(list, 1)

    val rdd1: RDD[(Int, String)] = add.map((_, "必须是对偶元组"))

    //写
    rdd1.saveAsSequenceFile("seq")

  }


  /**
   * 读SequenceFile
   */
  @Test
  def test5 () : Unit = {

    val rdd: RDD[(Int, String)] = sc.sequenceFile[Int, String]("seq")

    rdd.foreach(println)

  }


  /**
   * 读数据库
   * JdbcRDD[T: ClassTag](
   *     sc: SparkContext,                //spark上下文
   *     getConnection: () => Connection, //数据库JDBC连接
   *     sql: String,                     //sql查询语句
   *     lowerBound: Long,                 //指定sql语句中where判断的下界
   *     upperBound: Long,                  //指定sql语句中where判断的上界界
   *     numPartitions: Int,                //指定分区数
   *     mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _) //输出结果，结果中需要使用get方法
   */
  @Test
  def test6 () : Unit = {

    //创建RDD，RDD读取数据库中的数据
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver") //注册驱动
        val connection: Connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/chao", "root", "123456")
        connection                     //获取连接
      },
      "select * from table_user where age>=? and age< ?", 20, 40, 1,
      (rs: ResultSet) => {
        rs.getString("name") + "-----" + rs.getInt("age")
      }
    )
    rdd.collect().foreach(println)


  }

  /**
   * 写数据库
   */
  @Test
  def test7(): Unit = {

    val list = List(("Wade", 38), ("Kobe", 100), ("波兰周琦", 33))

    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //向数据库中写
    rdd.foreachPartition(it =>{

      //注册驱动
      Class.forName("com.mysql.jdbc.Driver")
      //获取连接
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/chao", "root", "123456")

      val ps: PreparedStatement = connection.prepareStatement("insert into table_user(name ,age) values(?,?)") //sql语句

      it.foreach({
        case (name,age) =>{
          ps.setString(1,name)
          ps.setInt(2,age)
          ps.execute()
        }
      })

      ps.close()//

      connection.close()//关闭数据库连接
    })

  }

  /**
   * 读HBase
   *        输入格式： TableInputFormat:读取一个表中的数据，封装为K-V对
   *                      RecordReader:[]
   *
   */
  @Test
  def test8 () : Unit = {

    //需要有和HBase相关的配置对象，通过配置对象获取hbase所在集群的地址
    val conf: Configuration = HBaseConfiguration.create()

    //指定要用TableInputFormat读取哪个表
    conf.set(TableInputFormat.INPUT_TABLE,"t2")


    /*val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])

    rdd.map({

    })
*/
  }


  /**
   * 写HBase
   */
  @Test
  def test9 () : Unit = {

  }


}
