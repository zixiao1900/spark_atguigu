package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_json {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._


        // TODO 执行逻辑操作
        spark.sql("select * from json.`datas/user.json`").show()
        /*
        +---+--------+
        |age|username|
        +---+--------+
        | 11|zhangsan|
        | 12|    lisi|
        | 13|  wangwu|
        +---+--------+
         */

        // TODO DataFrame 是按行读取 所以每行要符合json格式
        val df: DataFrame = spark.read.json("datas/user.json")
        df.show()
        /*
        +---+--------+
        |age|username|
        +---+--------+
        | 11|zhangsan|
        | 12|    lisi|
        | 13|  wangwu|
        +---+--------+
         */

        // todo DataFrame => SQL
        df.createOrReplaceTempView("user")

        spark.sql("select * from user").show
        /*
        +---+--------+
        |age|username|
        +---+--------+
        | 11|zhangsan|
        | 12|    lisi|
        | 13|  wangwu|
        +---+--------+
         */
        spark.sql("select age, username from user").show
        /*
        +---+--------+
        |age|username|
        +---+--------+
        | 11|zhangsan|
        | 12|    lisi|
        | 13|  wangwu|
        +---+--------+
         */

        spark.sql("select avg(age) from user").show
        /*
        +--------+
        |avg(age)|
        +--------+
        |    12.0|
        +--------+
         */

        // todo DataFrame => DSL
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
        df.printSchema()
        /*
        root
         |-- age: long (nullable = true)
         |-- username: string (nullable = true)
         */
        df.select("age", "username").show
        /*
        +---+--------+
        |age|username|
        +---+--------+
        | 11|zhangsan|
        | 12|    lisi|
        | 13|  wangwu|
        +---+--------+
         */
        // $表示引用  todo 需要导入隐式转换  import spark.implicits._
        df.select($"age" + 1).show()
        /*
        +---------+
        |(age + 1)|
        +---------+
        |       12|
        |       13|
        |       14|
        +---------+
         */
        df.select('age + 1).show
        /*
        +---------+
        |(age + 1)|
        +---------+
        |       12|
        |       13|
        |       14|
        +---------+
         */
        df.filter('age > 12).show()
        /*
        +---+--------+
        |age|username|
        +---+--------+
        | 13|  wangwu|
        +---+--------+
         */
        df.groupBy("age").count().show()
        /*
        +---+-----+
        |age|count|
        +---+-----+
        | 12|    1|
        | 11|    1|
        | 13|    1|
        +---+-----+
         */


        // TODO 关闭环境
        spark.close()
    }

}
