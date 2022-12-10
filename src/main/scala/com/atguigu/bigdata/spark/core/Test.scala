package com.atguigu.bigdata.spark.core

import scala.collection.mutable

object Test {

    def main(args: Array[String]): Unit = {
        println("Hello Spark")
        println("this is the first time")
        var num: Int = 10
        num += 5
        println(num)

        val map: mutable.Map[String, Int] = mutable.Map()
        map.update("a", 1)
        map.update("b", 2)
        map.update("a", 3)
        println(map)

    }
}
