package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  Created by chao-pc  on 2020-07-10 11:33
 *
 *        Hadoop MR WordCount:
 *              ①Configuration conf = new Configuration()
 *                  用来读取加载定义的MR程序的各种配置参数
 *
 *              ②Job job = Job.getInstance(conf);
 *                    Job作为当前程序的抽象表示！
 *                    通过Job来定义程序需要用到哪些组件
 *                    job.setMapper()
 *                    job.setReducer()
 *
 *              ③job.waitForCompletion
 *                    初始化 JobContextImpl():  Job的上下文实现
 *                    JobContextImpl 所有的内容，传递Mapper的Context，Reducer的Context
 *                            Context： 上下文
 *                                上文： 当前Job是怎么定义的，怎么配置的
 *                                下文:  当前Job要提交到哪个集群，以什么方式运行，下一步该运行什么组件
 *
 *                                Job的环境
 *
 *                     运行模式：  mapred-site.xml   mapreduce.framework.name
 *                              ①local(默认)
 *                              ②YARN
 *
 *
 *        Spark WC :
 *                 ①创建一个配置文件对象，读取Spark程序的默认配置和用户自定义的配置
 *                 ②需要有程序的上下文Context对象，通过Context来执行程序
 *
 *                    运行模式：
 *
 *                          ①local(本地)
 *                          ②Standalone(独立)
 *                          ③YARN，MESORSKBS
 *
 *        高阶函数： 算子
 *              分类：   转换算子：  如 map，groupby
 *                              是一种懒加载，lazy
 *                      行动算子： 调用行动算子，程序才开始运算！
 *
 *
 *
 */
object WordCount {
  com.atguigu.spark.day01.WordCount
  def main(args: Array[String]): Unit = {

    /*
          创建一个配置文件对象
              创建对象后，自动读取系统中默认的和Spark相关的参数，用户也可以自定义，覆盖默认参数

              setMaster：设置程序在哪个集群运行，设置要去哪的Master进程
                  local：本地模式

              setAppName：类似Job.setName()
                        给程序起名称

     */
    //val conf = new SparkConf().setMaster("local").setAppName("My app")
    val conf = new SparkConf().setAppName("My app") //集群中运行

    /*
        SparkContext:  一个应用中最核心的对象！
                        可以连接Spark集群，创建编程需要的RDD,累加器，广播变量

                        RDD: 弹性分布式数据集，简单理解为就是一个集合List
    */
    val sparkContext = new SparkContext(conf)

    //将文件中的每一行读取到集合
    //RDD[String]  类比为List[String]
    //val rdd: RDD[String] = sparkContext.textFile("input")
    val rdd: RDD[String] = sparkContext.textFile(args(0)) //集群中运行

    //将集合中的每个List使用空格切开，再扁平化将每个单词保存
    val words: RDD[String] = rdd.flatMap(x => x.split(" "))

    /*//对集合进行分组得出map
    //val wordMap: RDD[(String, Iterable[String])] = words.groupBy(x => x)

    //统计map中value的长度（即每个单词出现的次数）
    //val wordCount: RDD[(String, Int)] = wordMap.map(x => (x._1, x._2.size))*/

    //将每个单词转化为对偶元组，._2都为1
    val wordsConversion: RDD[(String, Int)] = words.map(x => (x, 1))

    /*
        reduceByKey:
            reduce: 将集合中的一堆数据，两两计算，得到一个结果
            reduceByKey ：基于key，将相同的key的value进行reduce运算

            适合(key,value)(key,value)
     */
    //按照key对每个._2进行归约
    val wordCount: RDD[(String, Int)] = wordsConversion.reduceByKey((x, y) => x + y)

    //调用行动算子
    val result: Array[(String, Int)] = wordCount.collect()

    println(result.mkString(","))//使用逗号隔开进行输出

    //关闭上下文
    sparkContext.stop()


  }

}
