package com.atguigu.spark.day09

/**
 *  Created by chao-pc  on 2020-07-24 21:11
 */
object DemonThreadTest {

  def main(args: Array[String]): Unit = {

    val myThread = new MyThread

    val thread = new Thread(myThread, "分线程")

    //设置当前线程为守护线程
    //如果一个JVM中只有守护线程,虚拟机就关闭!
    thread.setDaemon(true)

    thread.start()

    Thread.sleep(2000)  //main线程休眠两秒之后只剩下了守护线程,故在休眠2秒之后直接关闭

    println(Thread.currentThread().getName + "------>启动了!")
  }
}

class MyThread extends Runnable{
  override def run(): Unit = {

    println(Thread.currentThread().getName + "--->启动了!")

    for (i <- 1 to 10){

      Thread.sleep(1000)

      println(Thread.currentThread().getName + "--->" + i)

    }
  }
}
