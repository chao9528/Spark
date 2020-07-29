package com.atguigu.spark.day09

/**
 *  Created by chao-pc  on 2020-07-23 15:20
 *
 * Abstract class of a receiver that can be run on worker nodes to receive external data. A
 * custom receiver can be defined by defining the functions `onStart()` and `onStop()`. `onStart()`
 * should define the setup steps necessary to start receiving data,
 * and `onStop()` should define the cleanup steps necessary to stop receiving data.
 * Exceptions while receiving can be handled either by restarting the receiver with `restart(...)`
 * or stopped completely by `stop(...)`.
 *
 *    自定义:  ①定义onStart():完成开始接收数据前的准备工作!
 *            ②定义onStop():完成停止接收数据前的清理工作!
 *            ③异常时,可以调用restart(...),重启接收器或调用stop(),彻底停止!
 *            ④调用store()存储收到的数据
 *
 *             注意: onstart()不能阻塞,收数据需要在新线程中接收!
 *                    异常时,在任意线程都可以调用停止,重启,报错等方法!
 */



import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.junit._

class CustomReceiver {

  @Test
  def test1 () : Unit = {



  }

}

/**
 * 模拟HelloWorld中,通过一个接口源源不断的发送数据
 */
class MyReceiver(val host:String ,val port:String  ) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  /*
    收数据前的准备工作:
   */
  override def onStart(): Unit = {

    try {
      //val socket = new Socket(host, port)
    } catch {
      case  e => restart("需要重新连接")
    }



  }

  /*
    停止接收数据前的清理工作!
   */
  override def onStop(): Unit = {

    //关闭socket

  }

  def receiverData()={

  }

}




