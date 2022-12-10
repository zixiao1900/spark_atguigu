package com.atguigu.bigdata.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {

    def main(args: Array[String]): Unit = {
        // 连接服务器
        val client1 = new Socket("localhost", 9999)
        val client2 = new Socket("localhost", 8888)



        // 输出流  向服务器输出东西
        val out1: OutputStream = client1.getOutputStream
        val objOut1 = new ObjectOutputStream(out1)

        // 数据和计算
        val task = new Task()
        val subTask = new SubTask()
        subTask.logic = task.logic // 定义好计算
        subTask.datas = task.datas.take(2) // 定义好数据

        // 把计算和数据传给服务器  传的是对象 一定要序列化
        objOut1.writeObject(subTask)
        objOut1.flush()
        objOut1.close()
        client1.close()

        val out2: OutputStream = client2.getOutputStream
        val objOut2 = new ObjectOutputStream(out2)

        val subTask1 = new SubTask()
        subTask1.logic = task.logic
        subTask1.datas = task.datas.takeRight(2)
        objOut2.writeObject(subTask1)
        objOut2.flush()
        objOut2.close()
        client2.close()

        println("客户端数据发送完毕")
    }
}
