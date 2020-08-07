package com.hailian.scala.kafka

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._
import scala.collection.mutable

object KafkaZookeeperCheckPoint {

  // ZK client :lx-cs-04:2181,lx-cs-05:2181,lx-cs-06:2181
  var client: CuratorFramework = null
  // offset 路径起始位置  /kafkauat/consumers
  var Globe_kafkaOffsetPath:String = null
  //初始化zk访问
  def init(zklist: String, consumerPath: String) = {
    client = CuratorFrameworkFactory
      .builder
      .connectString(zklist)
      .sessionTimeoutMs(30000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()
    client.start()
    Globe_kafkaOffsetPath = consumerPath
  }

  // 路径确认函数  确认 ZK 中路径存在，不存在则创建该路径
  def ensureZKPathExists(path: String) = {

    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }
  }

  // 保存 新的 offset
  def storeOffsets(offsetRange: Array[OffsetRange], groupName: String) = {
    var Nums: Long = 0L

    for (o <- offsetRange) {
      if (o.fromOffset != null && o.untilOffset != null) Nums = o.untilOffset - o.fromOffset else Nums = 0L
      println("offsetRangeAll:" + o.toString())
      val zkPath = s"${Globe_kafkaOffsetPath}/${groupName}/${"offsets"}/${o.topic}/${o.partition}"
      // 检查路径是否存在
      ensureZKPathExists(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      println(zkPath + "---Offset写入ZK---读取量：" + Nums + "\nTopic：" + o.topic + ", Partition:" + o.partition + ", Offset:" + o.untilOffset)
      client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }

  def getFromOffset(topic: Array[String], groupName: String): (Map[TopicPartition, Long], Int) = {

    // Kafka 0.8 和 0.10 的版本差别，0.10 为 TopicPartition   0.8 TopicAndPartition
    var fromOffset: Map[TopicPartition, Long] = Map()

    val topic1 = topic(0).toString

    // 读取 ZK 中保存的 Offset，作为 Dstrem 的起始位置。如果没有则创建该路径，并从 0 开始 Dstream
    val zkTopicPath = s"${Globe_kafkaOffsetPath}/${groupName}/${"offsets"}/${topic1}"

    // 检查路径是否存在
    ensureZKPathExists(zkTopicPath)

    // 获取topic的子节点，即 分区
    val childrens = client.getChildren().forPath(zkTopicPath)

    // 遍历分区
    val offSets: mutable.Buffer[(TopicPartition, Long)] = for {
      p <- childrens
    }
      yield {

        // 遍历读取子节点中的数据：即 offset
        val offsetData = client.getData().forPath(s"$zkTopicPath/$p")
        // 将offset转为Long
        val offSet = java.lang.Long.valueOf(new String(offsetData)).toLong
        // 返回  (TopicPartition, Long)
        (new TopicPartition(topic1, Integer.parseInt(p)), offSet)
      }
    println(offSets.toMap)

    if (offSets.isEmpty) {
      (offSets.toMap, 0)
    } else {
      (offSets.toMap, 1)
    }


  }

  def createMyZookeeperDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, Object], topic: Array[String],
                                         groupName: String): InputDStream[ConsumerRecord[String, String]] = {

    // get offset flag = 1  表示基于已有的 offset 计算  flag = (null) 表示从头开始(最早或者最新，根据Kafka配置)
    val (fromOffsets, flag) = getFromOffset(topic, groupName)
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    if (flag == 1) {
      // 加上消息头
      //val messageHandler = (mmd:MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      println(fromOffsets)
      /*
          val stream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
       */
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams, fromOffsets))
      println(fromOffsets)
      println("中断后 Streaming 成功！")

    } else {
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams))

      println("首次 Streaming 成功！")

    }
    kafkaStream
  }


}
