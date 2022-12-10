package com.atguigu.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {

    def main(args: Array[String]): Unit = {

        // 启动服务器，接收数据
        val server = new ServerSocket(9999)
        println("服务器启动，等待接收数据")

        // 等待客户端的连接
        val client: Socket = server.accept()
        // 获取从客户端的输入
        val in: InputStream = client.getInputStream
        // 对象输入流
        val objIn = new ObjectInputStream(in)
        // 接受客户端的计算和数据  接受的是序列化后的对象
        val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
        val ints: List[Int] = task.compute() // 进行计算
        println("计算节点[9999]计算的结果为：" + ints)
        objIn.close()
        client.close()
        server.close()
    }
}
