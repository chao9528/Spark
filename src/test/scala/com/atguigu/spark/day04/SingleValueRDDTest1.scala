package com.atguigu.spark.day04

/**
 *  Created by chao-pc  on 2020-07-13 18:20
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

class DoubleValueRDDTest {

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

  @Test
  def test1 () : Unit = {
    val list = List(Person("James", 35), Person("Curry", 13), Person("Harden", 30))

    val rdd: RDD[Person] = sc.makeRDD(list, 2)

    rdd.sortBy(p => p.age ,false).saveAsTextFile("output")

  }


  /*
      scala中提供了Ordering:  extends Comparator[T]
                      比较器。比较两个对象时，通过一个比较器的实例，调用的是比较器中的compareTo方法！

                  Ordered: with java.lang.Comparable[A]
                      当前的类是可排序的，当前类已经实现了排序的方法，在比较时，直接调用类的compareTo方法！
   */
  @Test
  def test2 () : Unit = {
    val list = List(Person1("James", 35), Person1("Curry", 13), Person1("Harden", 30))

    val rdd: RDD[Person1] = sc.makeRDD(list, 2)

    rdd.sortBy(p => p,false).saveAsTextFile("output")
  }




}
//样例类
case class Person( name:String,age:Int )

case class Person1( name:String,age:Int ) extends Ordered[Person1]{
  //升序比较name
  override def compare(that: Person1): Int = this.name.compareTo(that.name)
}