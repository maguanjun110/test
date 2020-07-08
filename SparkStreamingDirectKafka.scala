package day16

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * 直连方式
  * 将offset保存在zk中
  */
object SparkStreamingDirectKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Direct").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Duration(5000))
    // 编写一些基本配置信息
    // 指定组名
    val groupId = "gp1"
    // 指定Topic
    val topic = "gp1918a"
    // 指定kafka的broker地址（之前我们指定zk，是因为，我们消费数据的时候需要去zk获取offset和元数据信息）
    val brokerList = "192.168.28.128:9092,192.168.28.129:9092,192.168.28.130:9092"
    // 指定zk的地址，用于保存offset，和之前的zk不是一样的，完全没关系
    val zks = "192.168.28.131:2181,192.168.28.131:2182,192.168.28.131:2183"
    // 创建Topic集合，因为有可能会消费多个topic下面的数据
    val topics = Set(topic)
    // 设置kafka的相关参数
    val kafkas = Map(
      "metadata.broker.list"->brokerList,
      "group.id"->groupId,
      "auto.offset.reset"->kafka.api.OffsetRequest.SmallestTimeString
    )
    // 开始做offset保存前期工作（手动维护Offset）
    val topicDirs = new ZKGroupTopicDirs(groupId,topic)
    // 首先获取zk中的路径 "/consumers/gp1/offsets/gp1918a"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    // 获取zkClient 可以获取偏移量数据，并且用于后面的更新
    val zkclient = new ZkClient(zks)
    // 查询一下该路径下面有没有对应目录，将查出里面的分区数（最大分区数）
    val client = zkclient.countChildren(zkTopicPath)
    // 创建DStream
    var kafkaStream:InputDStream[(String,String)] = null
    //如果zk中保存offset，我们会用offset作为起始位置
    // topicpartition  offset
    var fromOffsets: Map[TopicAndPartition,Long] = Map()
    // 如果我们zk有保存过Offset
    if(client > 0){
      // 如果有保存offset，那么将先循环我们的client
      for(i <-0 until client){
        // /consumers/gp1/offsets/gp1918a/0
        // /consumers/gp1/offsets/gp1918a/1
        // /consumers/gp1/offsets/gp1918a/2
        // offset取值
        val partitionOffset= zkclient.readData(s"${zkTopicPath}/${i}")
        // 将不同partition对应的Offset增加到fromOffsets
        // 创建一个TopicAndPartition
        val topicAndPartition = TopicAndPartition(topic,i)
        // 将数据加入进去
        fromOffsets +=(topicAndPartition -> partitionOffset.toString.toLong)
      }
      // 创建函数
      // messageHandler: JFunction[MessageAndMetadata[K, V], R]
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>{
        (mmd.key(),mmd.message())
      }
      // 接下来进行消费kafka数据
      // key  value 两种解码器
      kafkaStream = KafkaUtils.createDirectStream
        [String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkas,fromOffsets,messageHandler)
    }else{
       // 如果未保存过offset，根据kafka的配置从头消费数据
      kafkaStream = KafkaUtils.createDirectStream
        [String,String,StringDecoder,StringDecoder](ssc,kafkas,topics)
    }
    // 用于后面更新Offset
    var offsetRanges = Array[OffsetRange]()
    // 在进行业务处理
    kafkaStream.foreachRDD { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 对RDD数据进行操作
      rdd.map(_._2).foreach(x => println(x + "000000000000000"))
      // 更新偏移量
      for (o <- offsetRanges){
        // 取值 (路径)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        // 将该partition的offset保存到zk中
        ZkUtils.updatePersistentPath(zkclient,zkPath,o.untilOffset.toString)
      }
    }
    // 启动程序
    ssc.start()
    ssc.awaitTermination()
  }
}
