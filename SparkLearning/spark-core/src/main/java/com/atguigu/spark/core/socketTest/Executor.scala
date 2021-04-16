package com.atguigu.spark.core.socketTest

import java.io.ObjectInputStream
import java.net.ServerSocket


object Executor {

  def main(args: Array[String]): Unit = {

    //启动服务器，接收数据
    val serverSocket = new ServerSocket(9999)
    println("服务器启动，等待接收数据")

    val client = serverSocket.accept()
    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val subTask = objIn.readObject.asInstanceOf[SubTask]

    val res = subTask.compute()

    println("计算任务完成：" + res)
    objIn.close()
    in.close()
    client.close()
    serverSocket.close()
  }

}
