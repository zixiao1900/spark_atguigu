package com.atguigu.bigdata.spark.MLlib

import breeze.linalg.sum
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, collect_list, collect_set, concat_ws, lit, rank, row_number, size}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.sparkproject.dmg.pmml.False

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Swing {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    try {

      // 读取数据
      val entryDF: DataFrame = sc.textFile("datas/movielens_10000.csv").map(
        line => {
          val data: Array[String] = line.split("\\^")
          (data(0), data(1))
        }
      ).toDF("userId", "docId")
      entryDF.persist(StorageLevel.MEMORY_AND_DISK)

      // userId 对应浏览序列  userid, [item_id1, item_id2, item_id3]
      val user2itemListDF = entryDF.groupBy("userId")
        .agg(collect_set("docId").as("itemid_set"))
        .filter(size(col("itemid_set")) > 1)

      // item_list展开 遍历所有Pair组合   ((item_id1, item_id2), 1)
      val itemPairWithOneDF = user2itemListDF.flatMap(
        row => {
          val itemlist = row.getAs[mutable.WrappedArray[String]]("itemid_set").toArray
          val result: ArrayBuffer[(String, String, Long)] = ArrayBuffer()
          for (i <- 0 to itemlist.length - 2) {
            for (j <- i + 1 until itemlist.length) {
              result += ((itemlist(i), itemlist(j), 1))
            }
          }
          result
        }
      ).withColumnRenamed("_1", "itemidI")
        .withColumnRenamed("_2", "itemidJ")
        .withColumnRenamed("_3", "score")


      // 统计共现次数 过滤只有一次共现的 就是至少两个人同时点过这两个物品  item_id1, item_id2
      val itemPairDF = itemPairWithOneDF.groupBy("itemidI", "itemidJ")
        .sum("score").filter(col("sum(score)") >= 2).drop("sum(score)")

      // 物料表倒排 docid_0, [user_id1, user_id2,...] 后面可以用于统计user_id1 user_id2 每对user组合有总共出现的次数
      val userListDF = entryDF.groupBy("docId").agg(collect_set("userId").as("userid_set"))
        .filter(size(col("userid_set")) > 1)
      userListDF.persist(StorageLevel.MEMORY_AND_DISK)

      // 分别与item2userlist关联 将itemI itemJ 对应的itemI_userset, itemJ_userset 关联进来
      // itemidI, itemidJ, userSetI, userSetJ
      val itemPairAndUserPairListDF = itemPairDF
        .join(
          userListDF.withColumnRenamed("docId", "itemidI").withColumnRenamed("userid_set", "userid_set_I"),
          Seq("itemidI"),
          "inner"
        ).join(
        userListDF.withColumnRenamed("docId", "itemidJ").withColumnRenamed("userid_set", "userid_set_J"),
        Seq("itemidJ"),
        "inner"
      )

      // ,对每一个共现物料组合中的item_i,item_j，用两个user序列取交集
      // 表示itemi，itemj，useri，userj的一个帖子-用户四维共现矩阵 这里按行来看 item_pairs很可能重复  user_pair每行是唯一的组合
      val itemPairAndUserPair = itemPairAndUserPairListDF.flatMap { row =>
        val itemidJ = row.getAs[String]("itemidJ")
        val itemidI = row.getAs[String]("itemidI")
        val useridSetI = row.getAs[mutable.WrappedArray[String]]("userid_set_I").toArray
        val useridSetJ = row.getAs[mutable.WrappedArray[String]]("userid_set_J").toArray
        // 两个用户集合取交集 然后将所有用户可能的pair展开 侧写在item_pairs旁边
        val pairAccess = useridSetI.intersect(useridSetJ) // 取交集
        val result = new ArrayBuffer[(String, String, String, String)]()
        for (i <- 0 to pairAccess.length - 2) {
          for (j <- i + 1 until pairAccess.length) {
            result += ((itemidI, itemidJ, pairAccess(i), pairAccess(j)))
          }
        }
        result
      }.toDF("itemidI", "itemidJ", "useridI", "useridJ")


      // 有了itemPairAndUserPair后 还需要的就是这里每组user_pairs 一共共现的频率不知道 取得这个频率 关联在itemPairAndUserPair右边
      // 根据userListDF中的每个帖子对应的用户序列，构造出用户共现矩阵
      // dfitem2:用户共现矩阵  ((useri, userj), 1)
      val userPairWithOneDF = userListDF.flatMap { row =>
        val userlist = row.getAs[mutable.WrappedArray[String]](1).toArray
        val result = new ArrayBuffer[(String, String, Long)]()
        for (i <- 0 to userlist.length - 2) {
          for (j <- i + 1 until userlist.length) {
            result.append((userlist(i), userlist(j), 1))
          }
        }
        result // 将result展开,每一个元素一行
      }.withColumnRenamed("_1", "useridI").withColumnRenamed("_2", "useridJ").withColumnRenamed("_3", "user_pair_score")

      // 共同发生浏览的物料数量
      // 用户共现矩阵  user_id1, user_id2, 30
      val userPairWithRateDF = userPairWithOneDF.groupBy("useridI", "useridJ")
        .sum("user_pair_score")
        .withColumnRenamed("sum(user_pair_score)", "user_pair_score")

      // 将userPairRate 关联在 itemPairAndUserPair右边 并计算 1 / (alpha + userPairRate)
      val alpha = 1.0
      val itemPairWithSimilarDF = itemPairAndUserPair
        .join(userPairWithRateDF, Seq("useridI", "useridJ"), "inner")
        .withColumn("similar", lit(1.0) / (lit(alpha) + col("user_pair_score")))
        .drop("useridI", "useridJ", "user_pair_score")

      // 这里的item_pair 如果有(a, b) 就不会有(b, a) 要将 (a, b),(b, a) union起来 相对于一个item 才能让它从所有与它配对的items里面选出similar最高的N个
      val unionDF = itemPairWithSimilarDF.unionAll(
        itemPairWithSimilarDF.select(
          col("itemidI").as("itemidJ"),
          col("itemidJ").as("itemidI"),
          col("similar")
        ),
      )

      // 取topN
      val topN = 5
      val rcmDF = unionDF.groupBy("itemidI", "itemidJ").sum("similar")
        .withColumn("rank", row_number().over(Window.partitionBy("itemidI").orderBy(col("sum(similar)").desc)))
        .filter(col("rank") <= topN)
        .groupBy("itemidI").agg(concat_ws("#", collect_list("itemidJ")))
        .withColumnRenamed("collect_list(itemidJ", "rcmList")
        .withColumnRenamed("itemidI", "itemId")

      rcmDF.show(20, false)
      // 存csv
//      rcmDF.coalesce(1).write.option("sep", "^").csv("datas/swing_rcm_list.csv")

      entryDF.unpersist()
      userListDF.unpersist()

    } finally {
      spark.stop()
    }









  }
}
