package com.atguigu.bigdata.spark.streaming

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}
import scala.util.Random

object StreamingDataMockGenerator {
  def main(args: Array[String]): Unit = {
    val a = new Random().nextInt(3) // 0 - 2
    println(a)
    // todo 模拟流式数据  用文件顺序生成
    val cityInfo: Iterator[String] = mockDataFromFile("datas/city_info.txt")

    while (true) {
      // 随机生成数据
      val list1: ListBuffer[String] = mockdata()
      println("random generate: ")
      println(list1)

      // 从文件中 每次生成1-5条数据 按顺序往下生成
      val list2 = ListBuffer[String]()
      var num = new Random().nextInt(4) + 1 //  1 - 5条
      while (cityInfo.hasNext && num > 0) {
        list2.append(cityInfo.next())
        num -= 1
      }
      println("from file 1 - 5")
      println(list2)

      // 间隔两秒
      Thread.sleep(2000)
    }


  }

  def mockdata() = {
    val list: ListBuffer[String] = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华东", "华南")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")
    // 每次随机生成1-49条数据
    for (i <- 1 to new Random().nextInt(50)) {
      // 随机生成are, city
      val area: String = areaList(new Random().nextInt(3))
      val city: String = cityList(new Random().nextInt(3))
      // 1-6
      val userid: Int = new Random().nextInt(6) + 1
      val adid: Int = new Random().nextInt(6) + 1

      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    // timeStamp, area, city, userid, adid
    list
  }

  // todo 读取文件 返回生成器  可以通过iterator.next() 生成按文件行顺序成成一条数据
  def mockDataFromFile(path: String) = {
    val source: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = source.getLines()
    lines
  }
}
