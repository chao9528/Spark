package com.atguigu.spark.day05

/**
 *  Created by chao-pc  on 2020-07-14 11:11
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.junit._

class KeyValueRDDTest1 {
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


  /*
      sortBykey:根据key进行排序 :根据key进行排序    要求排序的key的类型必须是可以排序的类型

      sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)]
   */
  @Test
  def test1(): Unit = {

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    //转换为key-value对
    val rdd1: RDD[(Int, Int)] = rdd.map((_, 1))

    val rdd2: RDD[(Int, Int)] = rdd1.sortByKey(false)

    rdd2.saveAsTextFile("output")

  }



  @Test
  def test2 () : Unit = {

    val list = List(Person("James", 35), Person("Curry", 13), Person("Harden", 30))

    val rdd: RDD[Person] = sc.makeRDD(list, 1)

    //转换为key-value对
    val rdd1: RDD[(Person, Int)] = rdd.map((_, 1))

    val result: RDD[(Person, Int)] = rdd1.sortByKey()

    result.saveAsTextFile("output")

  }

  /*
    提供一个隐式的Ordering[K]
   */
  @Test
  def test3 () : Unit = {

    //提供一个隐式变量,供sortByKey使用
     implicit val ord: Ordering[Person1] = new Ordering[Person1] {
      //排序策略：先使用姓名升序，再使用年龄降序
      override def compare(x: Person1, y: Person1): Int = {
        val result: Int = x.name.compareTo(y.name)
        if (result == 0) {
          -x.age.compareTo(y.age)
        } else {
          result
        }
      }
    }

    val list = List(Person1("James", 35), Person1("Curry", 13), Person1("Harden", 30),Person1("Curry", 40))

    val rdd: RDD[Person1] = sc.makeRDD(list, 1)

    //转换为key-value对
    val rdd1: RDD[(Person1, Int)] = rdd.map((_, 1))

    val result: RDD[(Person1, Int)] = rdd1.sortByKey()

    result.saveAsTextFile("output")

  }


  @Test
  def test4 () : Unit = {

    val list = List(Person1("James", 35), Person1("Curry", 13), Person1("Harden", 30))

    val rdd: RDD[Person1] = sc.makeRDD(list, 1)

    //转换为key-value对
    val rdd1: RDD[(Int, Person1)] = rdd.map( p => (p.age,p))

    val result: RDD[(Int, Person1)] = rdd1.sortByKey()

    result.saveAsTextFile("output")

  }


  /*
      join :不同RDD中，将key相同的value进行关联   分区间有数据交换，有shuffle
      如果key存在不相等呢？
   */
  @Test
  def test5 () : Unit = {

    val list1 = List((1, "a"), (2, "b"), (3, "c"), (5, "e"))
    val list2 = List((1, "a1"), (2, "b1"), (3, "c1"), (4, "d1"))

    val rdd1: RDD[(Int, String)] = sc.makeRDD(list1, 2)
    val rdd2: RDD[(Int, String)] = sc.makeRDD(list2, 2)

    rdd1.join(rdd2).saveAsTextFile("output")

  }

  /*
      leftOuterJoin : 取左侧数据的全部和右边有关联的部分

          所有的Join都有shuffle
   */
  @Test
  def test6 () : Unit = {

    val list1 = List((1, "a"), (2, "b"), (3, "c"), (5, "e"))
    val list2 = List((1, "a1"), (2, "b1"), (3, "c1"), (4, "d1"))

    val rdd1: RDD[(Int, String)] = sc.makeRDD(list1, 2)
    val rdd2: RDD[(Int, String)] = sc.makeRDD(list2, 2)


    // (2,(b,Some(b1))) Some代表有，None代表无
    //             Option：选择
    //rdd1.leftOuterJoin(rdd2).saveAsTextFile("output")


    rdd1.fullOuterJoin(rdd2).saveAsTextFile("output") //满连接


  }

  /*
      cogroup:先在每个RDD内部，根据key进行聚合，将相同的key的value聚合为Iterable，再在rdd之间进行聚合
   */
  @Test
  def test7 () : Unit = {
    val list1 = List((1, "a"), (2, "b"), (3, "c"), (5, "e"),(1, "aa"), (2, "bb"))
    val list2 = List((1, "a1"), (2, "b1"), (3, "c1"), (4, "d1"), (3, "c11"), (4, "d11"))

    val rdd1: RDD[(Int, String)] = sc.makeRDD(list1, 2)
    val rdd2: RDD[(Int, String)] = sc.makeRDD(list2, 2)

    rdd1.cogroup(rdd2).saveAsTextFile("output")

  }


}

case class Person( name:String,age :Int) extends Ordered[Person]{
  //按照名称升序排列
  override def compare(that: Person): Int = this.age.compareTo(that.age)
}
case class Person1( name:String,age :Int) {
}





















