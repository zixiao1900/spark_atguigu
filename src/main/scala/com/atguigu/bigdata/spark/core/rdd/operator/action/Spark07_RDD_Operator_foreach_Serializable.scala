package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_foreach_Serializable {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List[Int](1,2,3,4))

        val user = new User()

        // 收集到内存中之后执行 没有问题
//        rdd.collect().foreach(num=>println(user.age + num))

        // SparkException: Task not serializable
        // NotSerializableException: com.atguigu.bigdata.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action$User

        // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能

        // todo 闭包检测  把对象发到executor  对象需要序列化
        rdd.foreach(
            num => {
                println("age = " + (user.age + num))
            }
        )

        sc.stop()

    }

    class User extends Serializable {
        // todo 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
        var age: Int = 30
    }

//    case class User() {
//        var age: Int = 30
//    }

//    class User {
//        var age : Int = 30
//    }
    // 报错 Exception in thread "main" org.apache.spark.SparkException: Task not serializable
    //	at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:416)

}
