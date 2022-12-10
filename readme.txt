日志问题
src/main/resources目录下有log4j.properties
target/classes目录下也有log4j.properties

IO winutils问题
下在winutils.exe放在hadoop/bin目录下 配置环境变量 hadoop/bin/winutils

spark-shell启动之后
http://Tom-Jerry:4040看jobs

Spark目录下提交:  例子  最后的10 是按顺序传入的参数 程序里面args(0)就能获得
bin\spark-submit --class org.apache.spark.examples.SparkPi --master local[*] --jars ./examples/jars/spark-examples_2.12-3.3.0.jar 10

flink在windows上启动
1. flink-1.8.0\bin>start-cluster.bat
localhost:8181上看jobs  执行了之后在task Managers里面看程序打印日志
2. flink-1.8.0\bin>flink.bat run ../examples/batch/WordCount.jar -input ../examples/batch/test.txt

standAlone模式
开启flick集群(不常用) .bin/start-cluster.sh
flink在linux上命令提交  -c后面写启动class相对路径 -p后面写并行度 --a 写a的传入参数
./bin/flink run -c com.atguigu.bigdata.flink.Flink001_stream_wordCount -p 2  D:\ScalaCodes\spark_atguigu\target\spark-core-1.0.0.jar --host localhost --port 9999
提交之后 在localhost:8181 RunningJobs里可以看
想终止任务 .bin/flink list可以查jobID  .bin/flick cancel jobID就可以终止任务
想看到所有的job  .bin/flink list -a
停止flink集群(不常用) ./bin/stop-cluster.sh

Yarn模式部署Flink任务：  上yarn看job的详细信息
    1 session模式 -n: TaskManager数量 -s:每个TaskManager的slot数量，默认一个slot一个core -jm: JobManager内存 -tm每个taskManager内存 -nm：yarn的appName -d:后台执行
        1) 启动  ./bin/yarn-session -n 2 -s 2 -jm 1024 -tm 1024 -rm test01 -d
        2) 提交job ./bin/flink run -c com.atguigu.bigdata.flink.Flink001_stream_wordCount D:\ScalaCodes\spark_atguigu\target\spark-core-1.0.0.jar --host localhost --port 9999
        3) 取消yarn-session  yarn application --kill application_132123_0001

    2 Per Job Cluster模式 -m yarn-cluster是根据当前的job起一个集群
        1) 直接执行job .bin/flink run -m yarn-cluster -c com.atguigu.bigdata.flink.Flink001_stream_wordCount D:\ScalaCodes\spark_atguigu\target\spark-core-1.0.0.jar --host localhost --port 9999



批任务就用Spark-core, spark-sql处理
流任务可以用spark-steaming(微批次), flink处理