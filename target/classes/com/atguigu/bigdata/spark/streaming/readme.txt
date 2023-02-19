SparkStreaming 准实时(秒，分钟) 微批次 的数据处理框架
DStream是随时间推移而收到的数据序列，在内部，每个时间区间收到的数据都作为RDD存在
而DStream是由这些RDD所组成的序列

数据流 -> 接收器 -> driver -> executor -> 在每个批次中输出结果

netcat工具像端口不断发送数据 模拟数据流
cmd：nc -lp 9999
cmd: 输入一行一行的数据

// 获取端口数据
val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
控制台打印每个微批次的数据处理结果

kafka相关操作

-- 启动zookeeper 不要关 在D:\WorkingTools\kafka_2.13-2.8.1目录下打开git bash
bash1: bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
-- 启动kafka  不要关  在D:\WorkingTools\kafka_2.13-2.8.1目录下打开git bash
bash2: bin/windows/kafka-server-start.bat ./config/server.properties

-- 创建topic 在D:\WorkingTools\kafka_2.13-2.8.1\bin\windows目录下打开cmd
cmd1: kafka-topics.bat --create --bootstrap-server localhost:9092 --topic topicCar --partitions 1 --replication-factor 1
return:
    Created topic topicCar.

-- 查看topic信息
cmd1: kafka-topics.bat --list --bootstrap-server localhost:9092

-- 生产者 发送消息
cmd1: kafka-console-producer.bat --broker-list localhost:9092 --topic topicCar
然后在cmd3中可以写消息发送

-- 接受消息
cmd2: kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic topicCar --from-beginning
cmd4中就会接受到cmd3中发送的消息

-- 关闭的时候 windows任务管理器 详细信息里面java开头的都kill掉

