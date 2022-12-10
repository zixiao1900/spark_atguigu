package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_SparkSQL_Hive {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")
        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder()
          .enableHiveSupport() // todo enableHiveSupport一定要写
          .config(sparkConf).getOrCreate()

        // todo 使用SparkSQL连接外置的Hive
        // 1. 拷贝Hive-size.xml文件到resources 和 target/classes目录下
        // 2. 启用Hive的支持
        // 3. 增加对应的依赖关系（包含MySQL驱动）

        // todo 内置hive  metasotre_db spark-warehouse 两个目录和内置hive相关
        // 创建表1
        spark.sql("show tables").show()
        /*
        +--------+---------+-----------+
        |database|tableName|isTemporary|
        +--------+---------+-----------+
        | default|  atguigu|      false|
        +--------+---------+-----------+
         */
        val df: DataFrame = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

        // 创建表2
        spark.sql("create table if not exists atguigu(id int)")
        // 加载数据到atuguigu
        spark.sql("load data local inpath 'datas/1.txt' overwrite into table atguigu")

        spark.sql("show tables").show
        /*
        +--------+---------+-----------+
        |database|tableName|isTemporary|
        +--------+---------+-----------+
        | default|  atguigu|      false|
        |        |     user|       true|
        +--------+---------+-----------+
         */
        spark.sql("select * from atguigu").show()
        /*
        +---+
        | id|
        +---+
        |  1|
        |  2|
        |  3|
        +---+
         */


        // TODO 关闭环境
        spark.close()
    }
}
