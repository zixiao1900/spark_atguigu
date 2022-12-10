package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // TODO DataSet
    // DataFrame其实是特定泛型的DataSet
    val seq = Seq(1,2,3,4)
    val ds0: Dataset[Int] = seq.toDS()
    ds0.show()
    /*
    +-----+
    |value|
    +-----+
    |    1|
    |    2|
    |    3|
    |    4|
    +-----+
     */

    // todo RDD => DataFrame   DataFrame就是DataSet[Row]
    val rdd = spark.sparkContext.makeRDD(
      List(
        (1, "zhangsan", 30),
        (2, "lisi", 40)
      )
    )
    val colNames: Array[String] = Array("id", "name","age")
    val df: DataFrame = rdd.toDF(colNames:_*)

    // todo DataFrame => RDD
    // todo df.rdd -> RDD[Row]   ds.rdd -> RDD[User]
    // todo df.rdd转过来的 类型都是Row 通过row.getAs(idx) 或者row.getAs("fieldName")  得到需要的字段值
    val rowRDD: RDD[Row] = df.rdd
    rowRDD.map ({
      row => {
        (row.getAs(0), row.getAs(1), row.getAs("age"))
      }
    }).foreach(println)
    /*
    (2,lisi,40)
    (1,zhangsan,30)
     */

    // todo DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    // todo RDD => DataSet
    // 直接转没有User这样的类型
    val ds00: Dataset[(Int, String, Int)] = rdd.toDS()
    // 类型转为User
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    // todo DataSet -> RDD
    val userRDD: RDD[User] = ds1.rdd
    val rdd1: RDD[(Int, String, Int)] = ds00.rdd

    spark.close()
  }
  case class User( id:Int, name:String, age:Int )
}
