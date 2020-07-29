package com.atguigu.spark.day05

/**
 *  Created by chao-pc  on 2020-07-15 15:09
 *
 * 1.闭包检测
 * 无闭包
 * 闭包检测
 * 解决
 * 2.属性序列化
 * 解决
 * 3.函数序列化
 * 解决
 *
 * 4.Kryo:    高效的Java的序列化框架！
 *
 *        java.io.Serializable：  保留除了列的属性值之外的一些其他信息，例如类的继承关系等等！
 *                  在大数据处理领域，不关心继承关系，只关心对象保存的值！
 *
 *          Kryo 节省 10倍网络传输！
 *
 *          使用：  ①要求类实现java.io.Serializable，闭包检查只检查是否实现了java.io.Serializable
 *                  ②使用kryo方式，序列化想序列化的类
 *
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.junit._

class SerializableTest {
  val conf = new SparkConf().setAppName("My app").setMaster("local[*]").registerKryoClasses(Array(classOf[User4]))
                                                                        // 在scala中获取一个类集Class类型：classof[类型]
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
      无闭包
      算子以外的代码都是在Driver端执行，算子里面的代码都是在Executor端执行。
      如果算子内用到了算子外的数据，这样就形成了闭包的效果，如果算子外的数据无法序列化，就意味着无法传值给Executor端执行，就会发生错误
      所以需要在执行任务计算之前，检测闭包内的对象是否可以进行序列化，这个操作称为闭包检测。
   */
  @Test
  def test1(): Unit = {
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    rdd.foreach(x => {
      val user = new User()
      println(x + "------------>" + user.age)
    })

  }

  /*
      有闭包： 如果算子存在闭包，那么在执行前，需要进行闭包检查，如果发现闭包中有
              不可序列化的变量，此时抛异常。

              一旦有异常，此时Job不会提交运行！

      解决： ①闭包变量序列化 extends Serializable
            ② 样例类 case class

   */
  @Test
  def test2(): Unit = {
    val list = List(1, 2, 33, 44)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val user = new User1()

    rdd.foreach(x => {
      //构成闭包
      println(x + "------------>" + user.age)
    })

  }


  /*
        一旦某个算子使用到了外部对象的属性，此时也构成闭包，要求对象的类也必须可以序列化！

            解决： 类 extends Serializable
   */
  @Test
  def test3(): Unit = {
    val list = List(1, 2, 33, 44)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val user = new User2()

    //调用matchAge间接调用了filter，使用user.age,构成闭包
    user.matchAge(rdd).collect().foreach(println)

  }


  /*
      涉及到类的方法的闭包，解决：
          ①类序列化
          ②使用匿名函数代替成员方法
   */
  @Test
  def test4(): Unit = {
    val list = List(1, 2, 33, 44)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val user = new User3()

    //调用matchAge间接调用了filter，使用user.fun(成员)，构成闭包
    user.matchAge(rdd).collect().foreach(println)
  }

  /*
    kryo：  高效的Java的序列化框架！

          java.io.Serializable：保留除了列的属性值之外的一些其他信息，例如类的继承关系等等！
                              在大数据处理领域，不关心继承关系，只关心对象保存的值！

           kryo  节省10倍网络传输！

           使用： ①要求类实现java.io.Serializable，闭包检查只检查是否实现了java.io.Serializable
                  ②使用kryo方式，序列化想序列化的类


   */
  @Test
  def test5(): Unit = {

    val list = List(User4(), User4(), User4(), User4(), User4(), User4(), User4(), User4(), User4(), User4(), User4(), User4())

    val rdd: RDD[User4] = sc.makeRDD(list, 2)

    val rdd1: RDD[(Int, User4)] = rdd.map(x => (1, x))

    val rdd2: RDD[(Int, Iterable[User4])] = rdd1.groupByKey()

    rdd2.collect() //本地查看	476.0 B  在SparkConf中设置了registerKryoClasses(Array(classOf[User4])之后  126.0 B

    Thread.sleep(1000000000)


  }


}

class User(val age: Int = 18) extends Serializable {}


case class User1(age: Int = 18)

//和属性相关
class User2(age: Int = 18) extends Serializable {
  def matchAge(data: RDD[Int]): RDD[Int] = {
    //局部变量和类的成员没有关系，不构成闭包


    val myAge = age //driver内的一个变量
    //在Executor
    data.filter(x => x < age)
  }

}

//和方法相关
class User3(age: Int = 18) extends Serializable {


  def fun(x: Int) = { //方法也是成员（函数式编程）
    x < age
  }

  def matchAge(data: RDD[Int]): RDD[Int] = {

    //在Executor
    data.filter(fun)
  }

}

case class User4(age: Int = 18, name: String = "curry") {}











