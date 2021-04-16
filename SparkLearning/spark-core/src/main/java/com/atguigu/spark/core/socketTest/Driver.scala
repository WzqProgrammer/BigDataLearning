package com.atguigu.spark.core.socketTest

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {
    //连接服务器
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 9998)

    val taskPool = new TaskPool()

    val out1 = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)
    val task1 = new SubTask()
    task1.data = taskPool.data.take(2)
    task1.logic = taskPool.logic
    objOut1.writeObject(task1)

    objOut1.flush()
    objOut1.close()

    val out2 = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)
    val task2 = new SubTask()
    task2.data = taskPool.data.takeRight(2)
    task2.logic = taskPool.logic
    objOut2.writeObject(task2)

    objOut2.flush()
    objOut2.close()

    client2.close()
    client1.close()
  }

}
