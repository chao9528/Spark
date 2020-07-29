package com.atguigu.spark.day03

/**
 *  Created by chao-pc  on 2020-07-13 15:28
 *
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._


class SingleValueRDDTest3 {

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


  /*
      sample :抽样。返回当前RDD的抽样子集！

      sample(
      withReplacement: Boolean,  //代表rdd中的元素是否允许被多次抽样！   true: 有可能被多次抽到， false:只能被抽到一次
      fraction: Double,    //样本比例   大致在这个范围
      seed: Long = Utils.random.nextLong): RDD[T]     //传入一个种子   如果想获取随机的效果，种子需要真随机  默认不写，即是真随机。

      在一个大型的数据集抽样查询，查看存在数据倾斜！
          某一个RDD中，有大key

      不会改变分区，也没有shuffle！

   */
  @Test
  def test1 () : Unit = {

    val list = List(1,2,3,4,5,6,7,8)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val result: RDD[Int] = rdd.sample(false, 0.5,2)
    val result2: RDD[Int] = rdd.sample(false, 0.5,2)
    val result3: RDD[Int] = rdd.sample(false, 0.5,2)
    val result4: RDD[Int] = rdd.sample(false, 0.5,2)

    println(result.collect().mkString(","))
    println(result2.collect().mkString(","))
    println(result3.collect().mkString(","))
    println(result4.collect().mkString(","))

  }

  /*
      distinct: 有shuffle
      底层处理数据源码: map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
   */
  @Test
  def test2 () : Unit = {

    val list = List(1,2,3,4,1,2,3,7,8,9)

    val rdd: RDD[Int] = sc.makeRDD(list, 3)

    //有shuffle
    rdd.distinct().saveAsTextFile("output")

  }

  /*
    不使用distinct，如何进行去重？
          groupby
   */
  @Test
  def test3 () : Unit = {

    val list = List(1,2,3,4,1,2,3,7,8,9)

    val rdd: RDD[Int] = sc.makeRDD(list, 3)

    val add1: RDD[(Int, Iterable[Int])] = rdd.groupBy(x => x)

    val result: RDD[Int] = add1.keys

    result.saveAsTextFile("output")

  }

  /*
      coalesce :合并
                    多分区  => 少分区，没有shuffle
                    少分区  => 大分区，有shuffle

                    coalesce默认不支持由少变多，由少变多，依然保持少的分区不变！

                    可以传入shuffle =true ，此时就会产生shuffle！重新分区！

                  总结:   ①coalesce默认由多的分区，合并到少的分区，不会产生shuffle
                          ②如果默认由少的分区，合并到多的分区，拒绝，还使用少的分区
                          ③由少的分区到多的分区，开启shuffle = true

      coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
      : RDD[T]

      窄依赖(narrow dependency)：当一个父分区，经过转换，产生一个子分区，此时称为窄依赖父分区！
                                独生子女
      宽依赖(Wide dependency)：当一个父分区，经过转换，产生多个子分区，此时称为宽依赖父分区！

      想扩大分区，怎么办？ 即shuffle = true

   */
  @Test
  def test4 () : Unit = {
    val list = List(1,2,3,4,1,2,3,7,8,9)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    rdd.coalesce(4,true).saveAsTextFile("output")

  }

  /*
      repartition
      coalesce和repartition的区别：
          repartition就是shuffle = true的coalesce
          repartition不管调大还是调小分区，都会shuffle！

      建议：调小分区使用coalesce
            调大分区使用repartition

   */
  @Test
  def test5 () : Unit = {
    val list = List(1,2,3,4,1,2,3,7,8,9)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    rdd.repartition(4)
  }

  /*
      sortBy :全排序    有shuffle
      sortBy[K](
      f: (T) => K,
      ascending: Boolean = true, //升降序
      numPartitions: Int = this.partitions.length) //是否重新分区
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
   */
  @Test
  def test6 () : Unit = {
    val list = List(1,2,3,4,1,2,3,7,8,9)

    val rdd: RDD[Int] = sc.makeRDD(list,2)

    //本质是调用sortByKey算子，使用RangePartitions进行分区
    rdd.sortBy(x=>x).saveAsTextFile("output")
  }

  /*
        pipe：允许每个分区都调用shell脚本处理RDD中的数据，返回输出的RDD


shell脚本:
#!/bin/sh
echo Start
while read LINE; do
  echo ">>>$LINE”
done
保存在/home/atguigu/下

进入本地spark的shell交互页面
scala> val list = List(1,2,3,4,1,2,3,7,8,9)
list: List[Int] = List(1, 2, 3, 4, 1, 2, 3, 7, 8, 9)

scala> sc.makeRDD(list,2)
res0: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at makeRDD at <console>:27

scala> res0.pipe("/home/atguigu/MyTest").collect()
res1: Array[String] = Array(Start, >>>1, >>>2, >>>3, >>>4, >>>1, Start, >>>2, >>>3, >>>7, >>>8, >>>9)

   */

}
