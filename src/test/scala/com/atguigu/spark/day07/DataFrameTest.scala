package com.atguigu.spark.day07

/**
 *  Created by chao-pc  on 2020-07-20 10:30
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit._

class DataFrameTest {
    //设置master
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")
    // 调用SparkSession.builder()
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    @After
    def stop {
        // 关闭session
        sparkSession.stop()
    }

    /*
            RDD 转 DataFrame
            提供了SparkSession
     */
    @Test
    def test1 () : Unit = {

        //设置master
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("df")

        // ①调用SparkSession.builder()
        val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        // 关闭session
        session.stop()
    }

    @Test
    def test2 () : Unit = {

        // 在SparkSession中直接设置master
        val session: SparkSession = SparkSession.builder().master("local[*]").appName("df").getOrCreate()

        // 关闭session
        session.stop()
    }

    /**
     * 创建DF，DF转RDD，DF就是DF的一个属性！
     *      DF.rdd()
     *
     *   type DataFrame = Dataset[Row]
     */
    @Test
    def test3 () : Unit = {

        val df: DataFrame = sparkSession.read.json("input/employees.json") // sparkSessio要读取的文件格式和名字

        df.show()

        //df 转RDD
        /*
                Row:输出的一行!可以看作是一个一行内容（多个字段）的集合

                可以使用Row(xx,xx,xx)构建一个Row
                可以使用Row(index)访问其中的元素
         */

        val rdd: RDD[Row] = df.rdd

        //val rdd1: RDD[(Any, Any)] = rdd.map(row => (row(0), row(1))) //通过索引获取每一行的值

        //val rdd2: RDD[(String, Long)] = rdd.map(row => (row.getString(0), row.getLong(1)))

        val rdd3: RDD[(String, Long)] = rdd.map({
            case a: Row => (a.getString(0), a.getLong(1))
        })

        rdd3.collect().foreach(println)

    }

    /**
     * RDD转DF
     *
     *      RDD出现的比DF早，RDD在编写源码时，不会提供对DF转换！
     *
     *      后出现的DF，又希望RDD可以对DF提供转换，需要扩展RDD类的功能！
     *
     *      解决： 为一个类提供扩展： ① 动态混入（非侵入式）
     *                             ② 隐式转换（非侵入式）  spark使用
     *
     *             ①在SparkSession.class类中，声明了一个objectimplicits。每创建一个SparkSession的对象，这个对象中，就包含一个
     *             名为implicitis的对象。
     *
     *             ②implicits extends SQLImplicits，在SQLImplicits，提供了rddToDatasetHolder,可以将一个RDD转为DatasetHolder
     *
     *             ③DatasetHolder提供toDS(),toDF()可以返回DS或DF
     *
     *         结论： 导入当前sparksession对象中的implicits对象中的所有方法即可！
     *                      import sparkSession(对象名).implicits._
     *
     *                      RDD.toDF()---->DatasetHolder.toDF()
     *
     *              object implicits extends SQLImplicits with Serializable {
     *                  protected override def _sqlContext: SQLContext = SparkSession.this.sqlContext
     *              }
     *
     *      implicit def rddToDatasetHolder[T : Encoder](rdd: RDD[T]): DatasetHolder[T] = {
     *          DatasetHolder(_sqlContext.createDataset(rdd))
     *      }
     *
     *
     */
    @Test
    def test4 () : Unit = {

        val list = List(1, 2, 3, 4, 5)

        val rdd: RDD[Int] = sparkSession.sparkContext.makeRDD(list, 2)

        //导入隐式转换的函数
        import  sparkSession.implicits._

        //默认列名为value，此处手动指定为num
        val df: DataFrame = rdd.toDF("num")

        df.show()

    }

    /**
     * 使用样例类由RDD转DF
     */
    @Test
    def test5 () : Unit = {

        val persons = List(Person("James", 200000.99), Person("Curry", 222000.99), Person("Wade", 1230000.99))

        val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(persons, 2)

        import  sparkSession.implicits._

        val df: DataFrame = rdd.toDF()

        df.show()


    }

    /**
     * 直接创建
     */
    @Test
    def test6 () : Unit = {

        /*
            def createDataFrame(rowRDD: RDD[Row], schema: StructType)

                rowRDD: 要转换的RDD的类型，必须是RDD[Row]
                schema: 结构！在schema中指定一行数据Row对象，含有几个字段，以及他们的类型

                        StructType的创建： StructType(List[StructField])

                StructField: 结构中的一个字段

               case class StructField(
                name: String,   必传
                dataType: DataType,   必传
                nullable: Boolean = true,
                metadata: Metadata = Metadata.empty)
                源码声明举例
                val struct =
                    StructType(
                    StructField("a", IntegerType, true) ::
                    StructField("b", LongType, false) ::
                    StructField("c", BooleanType, false) :: Nil)

         */
        val list = List(("James", 35), ("Curry", 18), ("Wade", 34))

        val rdd: RDD[(String, Int)] = sparkSession.sparkContext.makeRDD(list, 2)

        val rdd1: RDD[Row] = rdd.map {
            case (name, age) => Row(name, age)
        }

        val structType: StructType = StructType(StructField("username", StringType) :: StructField("age", IntegerType) :: Nil)

        val df: DataFrame = sparkSession.createDataFrame(rdd1, structType)

        df.show()

    }

}

case class Person(name:String,salary:Double)
