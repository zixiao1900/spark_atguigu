Application: 初始化一个SparkContext即生成一个Application
Job: 一个Action算子就会生成一个Job
Stage: Stage等于宽依赖ShuffleDependency的个数+1
Task:一个Stage阶段中，最后一个RDD的分区个数就是Task的个数
Application -> Job -> Stage -> Task 每一层都是1对n的关系